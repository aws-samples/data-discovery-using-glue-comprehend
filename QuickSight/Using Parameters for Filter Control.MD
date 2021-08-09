# Using Parameters for Filter Control

We can setup the required parameter with associated control to allow users to perform text-search for data-description.

1. From the left menu, Select [Parameters](https://docs.aws.amazon.com/quicksight/latest/user/parameters-set-up.html) 
2. Click on ( + ) to Create new parameter.
3. On “Create new parameter” wizard 
    1. Give a parameter name ex. `DataCatalogParameter` 
    2. On Data Type, choose “String”
    3. For Values, choose “Multiple values”
    4. Check for “Show as blank by default”
    5. Click Create.
4. On “Parameter Added” wizard 
    1. Associate it with [Control](https://docs.aws.amazon.com/quicksight/latest/user/parameters-controls.html) by choosing “Create a new control for a filter or a calculated field.”
5. On “Add control” wizard
    1. Give a display name ex. `Search Data Catalog`
    2. On style, you might select “Dropdown - multiselect”
    3. For Values, choose “Link to a dataset field”.
    4. On Dataset, select the dataset you have created at the first section “Data Catalog Query”
    5. For Fields, choose “comment”
    6. Click Add.

We should be able to see a new control being added into your analysis to allow users to search the comment field.

![alt text](https://github.com/aws-samples/data-discovery-using-glue-comprehend/blob/main/Images/QS_search1.png)


The last step, is to define a filter, hence whenever a user select a specific value(s) inside this search box, it will filter the catalog to provide only the relevant results to match this search.

1. Select the Table visual you have created in the previous section, and choose Filter
2. [Add filter](https://docs.aws.amazon.com/quicksight/latest/user/add-a-filter.html)by clicking ( + ) and select comment field.
3. A new filter is created with name `comment` , as of now it will includes all value.
4. We need to edit this filter to change its behaviour, so it only shows the table results matching the values for comment field given by users in the search box that we just created in the previous step.
    1. Click on the filter, to edit the filter.
    2. On “Edit Filter” wizard
        1. On Filter type, choose “[Custom Filter](https://docs.aws.amazon.com/quicksight/latest/user/add-a-text-filter.html#add-text-filter-custom-list)”
        2. Condition, choose “Equals”
        3. Check on “[Use Parameters](https://docs.aws.amazon.com/quicksight/latest/user/parameters-filtering-by.html)”
        4. On the pop-up that asks to confirm the “Change the scope of this filter?” you may select “Yes”
        5. On the Select a Parameter dropdown, choose the parameter that we have created in the previous section `DataCatalogParameter`
        6. Click Apply

Now you can start text search for data descriptions that contain (time)  where Amazon QuickSight can show the list of where these fields can be found within the organisation’s wide datasets stored within AWS S3 data lake, as shown below


![alt text](https://github.com/aws-samples/data-discovery-using-glue-comprehend/blob/main/Images/QS_search2.png)
