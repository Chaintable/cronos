package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/cosmos/iavl"
)

const legacyStorageKeyLength = 1 + 20 + 32

type legacyDeltaKind byte

const (
	legacyDeltaOld legacyDeltaKind = iota
	legacyDeltaNew
)

type legacyDelta struct {
	Version int64
	Key     []byte
	Kind    legacyDeltaKind
	Value   []byte
}

type byteRecordReader interface {
	Next() ([]byte, error)
}

func encodeLegacyDelta(delta legacyDelta) ([]byte, error) {
	if delta.Version < 1 {
		return nil, fmt.Errorf("legacy delta has invalid version %d", delta.Version)
	}
	if len(delta.Key) != legacyStorageKeyLength || delta.Key[0] != 0x02 {
		return nil, fmt.Errorf("legacy delta has invalid storage key length or prefix")
	}
	if delta.Kind != legacyDeltaOld && delta.Kind != legacyDeltaNew {
		return nil, fmt.Errorf("legacy delta has invalid kind %d", delta.Kind)
	}
	if delta.Kind == legacyDeltaOld && len(delta.Value) != 0 {
		return nil, fmt.Errorf("legacy old delta contains a value")
	}
	if delta.Kind == legacyDeltaNew && (len(delta.Value) == 0 || len(delta.Value) > 32) {
		return nil, fmt.Errorf("legacy new delta has invalid value length %d", len(delta.Value))
	}

	body := make([]byte, 8+legacyStorageKeyLength+2+len(delta.Value))
	binary.BigEndian.PutUint64(body[:8], uint64(delta.Version))
	copy(body[8:8+legacyStorageKeyLength], delta.Key)
	body[8+legacyStorageKeyLength] = byte(delta.Kind)
	body[8+legacyStorageKeyLength+1] = byte(len(delta.Value))
	copy(body[8+legacyStorageKeyLength+2:], delta.Value)
	return body, nil
}

func decodeLegacyDelta(body []byte) (legacyDelta, error) {
	const headerSize = 8 + legacyStorageKeyLength + 2
	if len(body) < headerSize {
		return legacyDelta{}, fmt.Errorf("legacy delta record is %d bytes, want at least %d", len(body), headerSize)
	}
	delta := legacyDelta{
		Version: int64(binary.BigEndian.Uint64(body[:8])),
		Key:     append([]byte(nil), body[8:8+legacyStorageKeyLength]...),
		Kind:    legacyDeltaKind(body[8+legacyStorageKeyLength]),
	}
	valueLength := int(body[8+legacyStorageKeyLength+1])
	if len(body) != headerSize+valueLength {
		return legacyDelta{}, fmt.Errorf("legacy delta record has value length %d and total size %d", valueLength, len(body))
	}
	delta.Value = append([]byte(nil), body[headerSize:]...)
	if _, err := encodeLegacyDelta(delta); err != nil {
		return legacyDelta{}, err
	}
	return delta, nil
}

func legacyDeltaLess(left, right []byte) bool {
	return bytes.Compare(left, right) < 0
}

func reduceLegacyDeltas(
	ctx context.Context,
	reader byteRecordReader,
	firstVersion, lastVersion int64,
	emit func(int64, *iavl.ChangeSet) error,
) error {
	if firstVersion < 1 || lastVersion < firstVersion {
		return fmt.Errorf("invalid legacy output range %d-%d", firstVersion, lastVersion)
	}

	nextBody, err := nextLegacyRecord(reader)
	if err != nil {
		return err
	}
	for version := firstVersion; version <= lastVersion; version++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		changeSet := &iavl.ChangeSet{}
		for nextBody != nil {
			first, err := decodeLegacyDelta(nextBody)
			if err != nil {
				return err
			}
			if first.Version < version {
				return fmt.Errorf("legacy delta version %d precedes output version %d", first.Version, version)
			}
			if first.Version > lastVersion {
				return fmt.Errorf("legacy delta version %d exceeds output range ending at %d", first.Version, lastVersion)
			}
			if first.Version != version {
				break
			}

			key := first.Key
			var oldSeen, newSeen bool
			var newValue []byte
			for nextBody != nil {
				delta, err := decodeLegacyDelta(nextBody)
				if err != nil {
					return err
				}
				if delta.Version != version || !bytes.Equal(delta.Key, key) {
					break
				}
				switch delta.Kind {
				case legacyDeltaOld:
					if oldSeen {
						return fmt.Errorf("legacy version %d has duplicate old storage key %x", version, key)
					}
					oldSeen = true
				case legacyDeltaNew:
					if newSeen {
						return fmt.Errorf("legacy version %d has duplicate new storage key %x", version, key)
					}
					newSeen = true
					newValue = append(newValue[:0], delta.Value...)
				}
				nextBody, err = nextLegacyRecord(reader)
				if err != nil {
					return err
				}
			}
			if !oldSeen && !newSeen {
				return fmt.Errorf("legacy version %d has empty delta group for key %x", version, key)
			}
			pair := &iavl.KVPair{Key: append([]byte(nil), key...)}
			if newSeen {
				pair.Value = append([]byte(nil), newValue...)
			} else {
				pair.Delete = true
			}
			changeSet.Pairs = append(changeSet.Pairs, pair)
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := emit(version, changeSet); err != nil {
			return err
		}
	}
	if nextBody != nil {
		return fmt.Errorf("legacy delta stream contains records after version %d", lastVersion)
	}
	return nil
}

func nextLegacyRecord(reader byteRecordReader) ([]byte, error) {
	body, err := reader.Next()
	if errors.Is(err, io.EOF) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if body == nil {
		return nil, fmt.Errorf("legacy delta reader returned a nil record without EOF")
	}
	return body, nil
}
