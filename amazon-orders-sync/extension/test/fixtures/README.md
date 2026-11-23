# Parser fixtures

These pages are **synthetic**: hand-written to mirror the structure of Amazon's order pages
(class names / `data-component` attributes taken from the current layouts and from the
selectors maintained by the MIT-licensed `amazon-orders` project). They contain no real
order data.

To add a real-world fixture after Amazon changes its markup:

1. Find a batch in `$DATALAKE_ROOT/raw/amazon/batches/` (default `/Volumes/MoErMedia/datalake.me`) whose record has the `raw_html` of the
   page that mis-parsed (or save the page from the browser with *Save Page As → HTML only*).
2. Replace names, addresses, order numbers and card digits with fake values.
3. Save it here and add an expectation in `test/parsers.test.js`.
