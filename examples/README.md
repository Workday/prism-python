# Workday Data Science Workflow

The goal of this project is to demonstrate use of the Workday Data Science Workflow. This workflow includes 3 steps:

1. Extract data from Workday using Report as a Service (RaaS)
2. Enrich your data using your desired Data Science tools
3. Push enriched data back into Workday via Prism API

## Example Use Case

This example demonstrates how to obtain survey responses via Workday RaaS, apply an Latent Dirichlet Allocation (LDA) topic model to the open-ended responses, and upload the predicted topics to Prism. This is meant to be a generic example of the workflow and should serve as inspiration of one way to integrate machine learning with Workday. 

## Prism Python Package

To upload your dataset to Prism, we recommend using the Python package `prism`.  This package makes it easy to programatically interact with the Prism API. To learn more about the Prism Python package, refer to the [package repository on GitHub](https://github.com/Workday/prism-python). 

To install the latest version of the Prism package:

```
pip install git+git://github.com/Workday/prism-python.git
```

> Note: when you install an additional package in Google Colab using this method, you will need to reinstall the package each time you launch a new session.
