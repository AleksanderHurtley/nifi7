# Kurs for Hong Kong — digifilm_600555_20130815

This example records the catalog exports and payloads from the Film Produksjon
run on nifi7 on 2026-11-19.

- [catalog/CATALOG.json](catalog/CATALOG.json) lists the six catalog exports
  captured from the finished SIP. The URN lookup returned one primary
  ITEM_PART, `70398032`, under ITEM `70398031`, MANIFESTATION `25600555`
  and WORK `600555`. The two direct sources are DPX `69447240` and WAV
  `69447242`. On nifi7, `URN_RESPONSE.xml` was byte-identical to the
  [primary export](catalog/PACKAGE_CATALOG_RECORD_70398032.xml).
- [submission.json](submission.json) contains the generated DPS submission
  body. The title and description come from WORK. Language is empty because
  the ITEM has no language entry. The MOV creation date is
  `2013-08-16T11:26:04Z`.
- [events.json](events.json) contains the three generated events in order.
  They cover MOV creation, transfer from SAM-FS with fixity verification, and
  SIP creation. The creation event uses the same timestamp as the submission.

Local replay of the captured XML reproduced the catalog manifest and the
catalog-derived submission metadata. The event and submission timestamps and
the reported MOV fixity were checked for agreement across the supplied records.
Independent verification of the creation date and file checksum requires the
original METS and MOV from the SIP.
