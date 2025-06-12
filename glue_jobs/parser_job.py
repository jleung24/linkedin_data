import sys
import os
import zipfile

# Add the zip to sys.path
added = False
for path in sys.path:
    if path.startswith("/tmp/glue-python-libs-") and os.path.isdir(path):
        for fname in os.listdir(path):
            if fname.endswith(".zip"):
                zip_path = os.path.join(path, fname)
                sys.path.insert(0, zip_path)
                print(f"Added {zip_path} to sys.path")
                # List contents
                with zipfile.ZipFile(zip_path, 'r') as z:
                    print("Zip contents:", z.namelist())
                added = True
                break
        if added:
            break

print("Before import linkedin_data")
try:
    import linkedin_data
    print("linkedin_data imported successfully")
except Exception as e:
    print(f"Import failed: {e}")
    import traceback
    traceback.print_exc()
    raise