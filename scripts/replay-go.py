"""Build the v0.6 archive with call callbacks on its pinned historical VM."""
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile


def prepare_overlay(vm, scratch):
    replacements = {}
    for name in ("evm.go", "instructions.go"):
        target = scratch / name
        shutil.copyfile(vm / name, target)
        replacements[str(vm / name)] = str(target)
        replacements[str((vm / name).resolve())] = str(target)
    patch = Path(__file__).resolve().with_name("replay-vm.patch")
    with patch.open("rb") as source:
        subprocess.run(["patch", "-s", "-p1"], cwd=scratch, stdin=source, check=True)
    overlay = scratch / "overlay.json"
    overlay.write_text(json.dumps({"Replace": replacements}))
    return overlay


def main():
    if sys.argv[1] == "--vendor-overlay":
        prepare_overlay(Path(sys.argv[2]).absolute(), Path(sys.argv[3]).absolute())
        return
    module = json.loads(subprocess.check_output(
        ["go", "list", "-m", "-json", "github.com/ethereum/go-ethereum"]
    ))["Replace"]
    if (module["Path"], module["Version"]) != (
        "github.com/crypto-org-chain/go-ethereum", "v1.10.3-patched"
    ):
        raise SystemExit("replay VM patch requires go-ethereum v1.10.3-patched")
    with tempfile.TemporaryDirectory(prefix="cronos-replay-build-") as directory:
        overlay = prepare_overlay(Path(module["Dir"]) / "core/vm", Path(directory).resolve())
        subprocess.run(["go", sys.argv[1], "-overlay", str(overlay), *sys.argv[2:]], check=True)


if __name__ == "__main__":
    main()
