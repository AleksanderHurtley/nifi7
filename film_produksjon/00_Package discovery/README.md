# List packages for database preparation

[01_List packages.groovy](01_List%20packages.groovy) creates one empty child
FlowFile per package, ready for the database-insert steps. It does not create
database rows or copy media.

The produksjon layout has two levels below the configured root:

```text
/global/film/00/film/produksjon/       <- inputDirectory
└── 202303/                           <- year/month
    └── digifilm_796646_20230321/      <- package: emit one FlowFile
        ├── digifilm_796646_20230321.xml
        ├── logs/
        ├── lossless/
        └── meta/
```

The bevaring discovery script had an additional directory level. Keeping its
third loop here would emit `logs`, `lossless`, and `meta` as packages.

## NiFi setup

```text
GenerateFlowFile → UpdateAttribute → ExecuteGroovyScript → DB preparation steps
```

1. Set the FlowFile attribute `inputDirectory` in `UpdateAttribute` to
   `/global/film/00/film/produksjon`.
2. Use this script as the `ExecuteGroovyScript` Script Body, or deploy it on
   nifi7 and configure Script File. Use one of those properties, not both.
3. Set concurrent tasks to `1` and keep **Failure Strategy = rollback**.
   On a cluster, ensure the trigger is produced once (for example, on the
   primary node), not independently on every node.
4. Connect `success` to the next preparation step and `failure` to a review
   queue. Run the trigger once to produce the inventory.

The example package produces these attributes:

```text
package.name = digifilm_796646_20230321
package.path = /global/film/00/film/produksjon/202303/digifilm_796646_20230321
```

Children inherit the trigger's attributes and have empty content. The trigger
is removed after successful listing. An empty source produces no children and
a log entry reporting zero packages. Missing/invalid/unreadable roots or a
directory-listing or entry-inspection failure route the trigger to `failure`
with `error.stage` and `error.message`, before any children are created. This
includes a month directory whose entries can be listed but whose permissions
prevent inspecting the package directories inside it.

The script lists immediate non-hidden directories at exactly two levels,
sorted by name. It skips regular files and directory symlinks. Like the
original script, it does not validate month/package naming patterns or package
completeness; it assumes the configured root has the layout shown above.
It does not list package contents or open media or metadata files.

Listing is stateless: each new trigger emits the same packages again. The
database preparation step must handle duplicate imports. This helper is
separate from the production flow's **Find new package in DB** group, which
will consume the rows after they have been created.

Unhandled session errors must roll back the batch. This is the documented
[ExecuteGroovyScript failure strategy](https://nifi.apache.org/components/org.apache.nifi.processors.groovyx.ExecuteGroovyScript/).
