---
title: How to connect to data on Azure Blob Storage using Spark
---

import NextSteps from '../../components/next_steps.md'
import Congratulations from '../../components/congratulations.md'
import Prerequisites from '../../components/prerequisites.jsx'
import WhereToRunCode from '../../components/where_to_run_code.md'
import SparkDataContextNote from '../../components/spark_data_context_note.md'
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

This guide will help you connect to your data stored on Microsoft Azure Blob Storage (ABS) using Spark.
This will allow you to <TechnicalTag tag="validation" text="Validate" /> and explore your data.

<Prerequisites>

- Have access to data on an ABS container
- Have access to a working Spark installation

</Prerequisites>

## Steps

### 1. Choose how to run the code in this guide

<WhereToRunCode />

### 2. Instantiate your project's DataContext

Import these necessary packages and modules.

```python name="tests/integration/docusaurus/connecting_to_your_data/cloud/azure/spark/inferred_and_runtime_yaml_example.py imports"
```

<SparkDataContextNote />

Please proceed only after you have instantiated your `DataContext`.

### 3. Configure your Datasource

Using this example configuration, add in your ABS container and path to a directory that contains some of your data:

<Tabs
  groupId="yaml-or-python"
  defaultValue='yaml'
  values={[
  {label: 'YAML', value:'yaml'},
  {label: 'Python', value:'python'},
  ]}>

<TabItem value="yaml">

```python name="tests/integration/docusaurus/connecting_to_your_data/cloud/azure/spark/inferred_and_runtime_yaml_example.py datasource config"
```

:::info Authentication

It is also important to note that ABS `DataConnector` for Spark supports the method of authentication called 
Windows Azure Storage Blob Secure ("WASBS"), which requires the `AZURE_ACCESS_KEY` environment variable to be set.

For more details regarding storing credentials for use with Great Expectations see: [How to configure credentials](../../../setup/configuring_data_contexts/how_to_configure_credentials.md)

For more details regarding authentication, please visit the following:
* [WASB Tutorial](https://datacadamia.com/azure/wasb)
* [Azure Authentication](https://docs.microsoft.com/en-us/azure/storage/common/storage-account-keys-manage)

:::

Run this code to test your configuration.

```python name="tests/integration/docusaurus/connecting_to_your_data/cloud/azure/spark/inferred_and_runtime_yaml_example.py test datasource"
```

</TabItem>

<TabItem value="python">

```python name="tests/integration/docusaurus/connecting_to_your_data/cloud/azure/spark/inferred_and_runtime_python_example.py datasource config"
```

Run this code to test your configuration.

```python name="tests/integration/docusaurus/connecting_to_your_data/cloud/azure/spark/inferred_and_runtime_python_example.py test datasource"
```

</TabItem>

</Tabs>

If you specified an ABS path containing CSV files you will see them listed as `Available data_asset_names` in the output of `test_yaml_config()`.

Feel free to adjust your configuration and re-run `test_yaml_config()` as needed.

### 4. Save the Datasource configuration to your DataContext

Save the configuration into your `DataContext` by using the `add_datasource()` function.

<Tabs
  groupId="yaml-or-python"
  defaultValue='yaml'
  values={[
  {label: 'YAML', value:'yaml'},
  {label: 'Python', value:'python'},
  ]}>

<TabItem value="yaml">

```python name="tests/integration/docusaurus/connecting_to_your_data/cloud/azure/spark/inferred_and_runtime_yaml_example.py add datasource"
```

</TabItem>

<TabItem value="python">

```python name="tests/integration/docusaurus/connecting_to_your_data/cloud/azure/spark/inferred_and_runtime_python_example.py add datasource"
```

</TabItem>

</Tabs>

### 5. Test your new Datasource

Verify your new <TechnicalTag tag="datasource" text="Datasource" /> by loading data from it into a <TechnicalTag tag="validator" text="Validator" /> using a <TechnicalTag tag="batch_request" text="Batch Request" />.

Add the name of the <TechnicalTag tag="data_asset" text="Data Asset" /> to the `data_asset_name` in your `BatchRequest`.

```python name="tests/integration/docusaurus/connecting_to_your_data/cloud/azure/spark/inferred_and_runtime_yaml_example.py batch request"
```

Then load data into the `Validator`.
```python name="tests/integration/docusaurus/connecting_to_your_data/cloud/azure/spark/inferred_and_runtime_yaml_example.py get validator"
```

<Congratulations />

## Additional Notes

To view the full scripts used in this page, see them on GitHub:

- [spark_configured_azure_yaml_example.py](https://github.com/great-expectations/great_expectations/blob/develop/tests/integration/docusaurus/connecting_to_your_data/cloud/azure/spark/configured_yaml_example.py)
- [spark_configured_azure_python_example.py](https://github.com/great-expectations/great_expectations/blob/develop/tests/integration/docusaurus/connecting_to_your_data/cloud/azure/spark/configured_python_example.py)
- [spark_inferred_and_runtime_azure_yaml_example.py](https://github.com/great-expectations/great_expectations/blob/develop/tests/integration/docusaurus/connecting_to_your_data/cloud/azure/spark/inferred_and_runtime_yaml_example.py)
- [spark_inferred_and_runtime_azure_python_example.py](https://github.com/great-expectations/great_expectations/blob/develop/tests/integration/docusaurus/connecting_to_your_data/cloud/azure/spark/inferred_and_runtime_python_example.py)

## Next Steps

<NextSteps />
