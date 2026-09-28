# SAM-FS source packages: video → eksternvideo

Keep small reference files and structure listings from SAM-FS → video → eksternvideo
here. These examples are tracked in Git and document the input layout before
the NiFi flow fetches or processes a package.

Copy only the small metadata files from SAM-FS when the media layout is known.
Record omitted media paths, sizes, and archive contents in the inventory.
Complete downloads are optional: [local-packages/](../../local-packages/README.md)
can hold full packages or partial metadata copies and is ignored by Git except
for its README.

Use one directory per package, keeping its original name and internal layout:

```text
examples/sam-fs/
├── README.md
├── <original-package-name>/
│   └── <selected small files in their original subdirectories>
├── <original-package-name>.files.txt
└── <original-package-name>.notes.md
```

Include the source metadata, manifests, and checksum files where available.
Keep filenames and directory names as they appear on SAM-FS.

In the sibling `<original-package-name>.notes.md`, record the original SAM-FS
path, the date the example was captured, and anything omitted or changed.
If large media files are omitted, include their relative paths and sizes in
the notes or a sibling `<original-package-name>.files.txt`. Record empty
directories there too, since Git does not track them.

## Available examples

- [no-nb_eksternvideo_AV1000026258_01](no-nb_eksternvideo_AV1000026258_01.notes.md):
  user-supplied inventories of the original MKV and reference TIFF archives.
  The absolute source path and metadata copy are pending.
