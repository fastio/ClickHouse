---
description: 'The table function allows to read data from the YTsaurus cluster.'
sidebar_label: 'ytsaurus'
sidebar_position: 85
title: 'ytsaurus'
doc_type: 'reference'
---


# ytsaurus Table Function

<ExperimentalBadge/>

The table function allows to read data from the YTsaurus cluster.

## Syntax 

```sql
ytsaurus(http_proxy_url, cypress_path, oauth_token, format)
```

:::info
This is an experimental feature that may change in backwards-incompatible ways in the future releases.
Enable usage of the YTsaurus table function
with [allow_experimental_ytsaurus_table_function](/operations/settings/settings#allow_experimental_ytsaurus_table_engine) setting.
Input the command `set allow_experimental_ytsaurus_table_function = 1`.
:::

## Arguments 

- `http_proxy_url` — URL to the YTsaurus http proxy.
- `cypress_path` — Cypress path to the data source.
- `oauth_token` — OAuth token.
- `format` — The [format](/interfaces/formats) of the data source.

**Returned value**

A table with the specified structure for reading data in the specified ytsaurus cypress path in YTsaurus cluster.

**See Also**

- [ytsaurus engine](/engines/table-engines/integrations/ytsaurus.md)
