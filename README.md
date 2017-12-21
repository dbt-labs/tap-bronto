# tap-bronto

Author: Connor McArthur (connor@fishtownanalytics.com)

This is a [Singer](http://singer.io) tap that produces JSON-formatted data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

It:

- Generates a catalog of available data in Bronto
- Extracts the following resources:
  - [Contacts](http://dev.bronto.com/api/soap/functions/read/readcontacts/) ([source](/tap_bronto/endpoints/contact.py))
  - [InboundActivityStream](http://dev.bronto.com/api/soap/functions/read/readrecentinboundactivities/) ([source](/tap_bronto/endpoints/inbound_activity.py))
  - [OutboundActivityStream](http://dev.bronto.com/api/soap/functions/read/readrecentoutboundactivities/) ([source](/tap_bronto/endpoints/outbound_activity.py))
  - [Lists](http://dev.bronto.com/api/soap/functions/read/readlists/) ([source](/tap_bronto/endpoints/list.py))
  - [Unsubscribes](http://dev.bronto.com/api/soap/functions/read/readunsubscribes/) ([source](/tap_bronto/endpoints/unsubscribe.py))

### Quick Start

1. Install

```bash
git clone git@github.com:fishtown-analytics/tap-bronto.git
cd tap-bronto
pip install .
```

2. Get an API from Bronto.
3. Create the config file.

There is a template you can use at `config.json.example`, just copy it to `config.json` in the repo root and insert your token.

4. Run the application to generate a catalog.

```bash
tap-bronto -c config.json --discover > catalog.json
```

5. Select the tables you'd like to replicate

Step 4 a file called `catalog.json` that specifies all the available endpoints and fields. You'll need to open the file and select the ones you'd like to replicate. See the [Singer guide on Catalog Format](https://github.com/singer-io/getting-started/blob/c3de2a10e10164689ddd6f24fee7289184682c1f/BEST_PRACTICES.md#catalog-format) for more information on how tables are selected.

6. Run it!

```bash
tap-bronto -c config.json --properties catalog.json
```

---

Copyright &copy; 2017 Fishtown Analytics
