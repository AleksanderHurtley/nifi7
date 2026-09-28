# SAM-FS source packages: film → produksjon

Keep small reference files and structure listings from SAM-FS → film → produksjon
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

- [digifilm_1300107_20130729](digifilm_1300107_20130729.notes.md): layout from
  user-provided terminal output, including a populated `logs/` directory and
  five `.done` files. No file contents or sizes have been captured.
- [digifilm_796646_20230321](digifilm_796646_20230321.notes.md): seven XML
  metadata files, a complete source directory inventory, and source-verified
  metadata checksums. The large MOV file is omitted.
