## Simplify Data Discovery for Business Users using AWS Glue and Amazon Comprehend

This Github repository is created to provide the sample source for the AWS blog post- Simplify Data Discovery for Business Users using AWS Glue and Amazon Comprehend

In this blog post we will discuss how to bridge the gap between Domain Data Experts and business focused consumer teams known for authoring BI reports and dashboards.

By allowing BI Authors who are using Amazon QuickSight to search and discover data stored in AWS data lake storage ,Amazon S3, through Amazon Athena, BI authors, can access metadata stored in AWS Glue Data Catalog. In addition to a simple column-level data description powered by Amazon Comprehend AI for automatically detected data-entities, while using Amazon Comprehend custom recognition where data descriptions are labelled by Domain Experts through Amazon SageMaker Ground Truth for the unidentified data-entities.

## Table of Contents
1. [Datasets sample](https://github.com/aws-samples/data-discovery-using-glue-comprehend/tree/main/Datasets).
2. [AWS CloudFormation Template](CloudFormation_template/template_trigger_glue_crawler.yaml)
3. [Lambda function to create AWS Glue DB,Crawler and  Glue Data Catalog](scripts/trigger_glue_crawler.py)
4. [Glue job to update the Data Catalog using Comprehend Detect PII API](scripts/Glue_Comprehend_Job.py)
5. [Lambda function that get automatically triggered once the AWS Glue Crawler successfully complete](scripts/glue_comprehend_workflow.py)
6. [Train Amazon Comprehend Custom entity](scripts/comprehend_create_custom_entity.py)
7. [Comprehend inference job] (scripts/Inference_custom_entity _recognition.py)
8. [Glue job to update the Data Catalog using Comprehend Custom entity](scripts/glue_comprehend_workflow_custom.py )

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.

