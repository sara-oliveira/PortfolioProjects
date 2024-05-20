-- Data Cleaning

-- Dataset from: https://www.kaggle.com/datasets/ratnarohith/uncleaned-bike-sales-data

-- Skills used: CTE's, Row Number, Partition By, Case Statement



# Checking everything that is within the sales table

SELECT *
FROM bike_sales.sales;


# To-do:
# 1. Standardize data and fix errors
# 2. Remove duplicates
# 3. Populate blank values
# 4. Remove unnecessary columns


# 1. Standardize data and fix errors
# This step consists in removing extra blank spaces and the dollar sign from some of the values
# For that, let's create a staging table, where I will perform all of the transformations. Let's also keep the raw dataset untouched, in case something happens

CREATE TABLE bike_sales.sales_staging
LIKE bike_sales.sales;

SELECT * 
FROM bike_sales.sales_staging;


# The sales_staging table has the exact same columns as the sales table, but no data. So, now I need to populate the sales_staging table

INSERT sales_staging
SELECT *
FROM bike_sales.sales;


# So, there are some blank spaces on the Country column, as it is possible to see with the following query

SELECT DISTINCT(Country)
FROM bike_sales.sales_staging;


# There are 4 different values for United States due to extra blank spaces before and after our value, so let's repair this

SELECT Country, TRIM(Country)
FROM bike_sales.sales_staging;


# Now, let's update our sales_staging table

UPDATE bike_sales.sales_staging
SET Country = TRIM(Country);


# If we check again the following query, we see that we still have 2 values for United States, due to extra blank spaces between the words 'United' and 'States'

SELECT DISTINCT(Country)
FROM bike_sales.sales_staging;


# In order to fix this, let's update the table using the REPLACE() function

SELECT 
REPLACE(
	REPLACE(
		REPLACE(Country, ' ', '<>'), 
        '><', 
        ''
	), 
    '<>', 
    ' '
) AS Country_New
FROM bike_sales.sales_staging;

UPDATE bike_sales.sales_staging
SET Country = 
	REPLACE(
		REPLACE(
			REPLACE(Country, ' ', '<>'), 
			'><', 
			''
		), 
		'<>', 
		' '
	);

SELECT DISTINCT(Country)
FROM bike_sales.sales_staging;


# Now, let's remove the dollar sign and the blank spaces from the columns Unit_Cost, Unit_Price, Profit, Cost and Revenue

SELECT *
FROM bike_sales.sales_staging;

SELECT 
REPLACE(
	REPLACE(Unit_Cost, '$', ''),
    ' ',
    ''
) AS Unit_Cost_New
FROM bike_sales.sales_staging;

UPDATE bike_sales.sales_staging
SET Unit_Cost = 
	REPLACE(
		REPLACE(Unit_Cost, '$', ''),
		' ',
		''
	)
;

UPDATE bike_sales.sales_staging
SET Unit_Price = 
	REPLACE(
		REPLACE(Unit_Price, '$', ''),
		' ',
		''
	)
;

UPDATE bike_sales.sales_staging
SET Profit = 
	REPLACE(
		REPLACE(Profit, '$', ''),
		' ',
		''
	)
;

UPDATE bike_sales.sales_staging
SET Cost = 
	REPLACE(
		REPLACE(Cost, '$', ''),
		' ',
		''
	)
;

UPDATE bike_sales.sales_staging
SET Revenue = 
	REPLACE(
		REPLACE(Revenue, '$', ''),
		' ',
		''
	)
;


# Just checking how the sales_staging table looks right now

SELECT *
FROM bike_sales.sales_staging;


# Let's just update the data type for the columns Unit_Cost, Unit_Price, Profit, Cost and Revenue
# Currently, they are considered as text, due to the dollar sign, so now let's update them as the decimal type

ALTER TABLE bike_sales.sales_staging MODIFY Unit_Cost DECIMAL;
ALTER TABLE bike_sales.sales_staging MODIFY Unit_Price DECIMAL;
ALTER TABLE bike_sales.sales_staging MODIFY Profit DECIMAL;
ALTER TABLE bike_sales.sales_staging MODIFY Cost DECIMAL;
ALTER TABLE bike_sales.sales_staging MODIFY Revenue DECIMAL;


# I've also noticed that there is a misspelling in the Month column, so let's also fix that
# Let's identify the row(s) with the misspelling

SELECT *
FROM bike_sales.sales_staging
WHERE `Month` <> "December";


# Now, let's update the table with the correct value

UPDATE bike_sales.sales_staging
SET `Month` = "December"
WHERE `Month` <> "December";


# Let's check our sales_staging table again

SELECT *
FROM bike_sales.sales_staging;




# 2. Remove duplicates
# In order to identify the duplicate rows, instead of using the sales_order (aka ID) column, let's do a Partition By every single column to count the number of occurrences for each row
# This will ensure that I'm really fetching the identical rows and not different rows with the same ID 

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY `Date`, `Day`, `Month`, `Year`, Customer_Age, Age_Group, Customer_Gender, Country, State, Product_Category, Sub_Category, Product_Description, Order_Quantity, Unit_Cost, Unit_Price, Profit, Cost, Revenue) AS row_num
FROM bike_sales.sales_staging;


# Checking if there's any row_num greater than 1 (which would be the duplicate rows)

WITH duplicate_cte AS 
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY `Date`, `Day`, `Month`, `Year`, Customer_Age, Age_Group, Customer_Gender, Country, State, Product_Category, Sub_Category, Product_Description, Order_Quantity, Unit_Cost, Unit_Price, Profit, Cost, Revenue
) AS row_num
FROM bike_sales.sales_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;


# Let's create a new table called sales_staging_2, in order to easily identify and delete all the rows where row_num > 1

CREATE TABLE `bike_sales`.`sales_staging_2` (
  `Sales_Order` text,
  `Date` text,
  `Day` int(11) DEFAULT NULL,
  `Month` text,
  `Year` int(11) DEFAULT NULL,
  `Customer_Age` int(11) DEFAULT NULL,
  `Age_Group` text,
  `Customer_Gender` text,
  `Country` text,
  `State` text,
  `Product_Category` text,
  `Sub_Category` text,
  `Product_Description` text,
  `Order_Quantity` int(11) DEFAULT NULL,
  `Unit_Cost` decimal(10,0) DEFAULT NULL,
  `Unit_Price` decimal(10,0) DEFAULT NULL,
  `Profit` decimal(10,0) DEFAULT NULL,
  `Cost` decimal(10,0) DEFAULT NULL,
  `Revenue` decimal(10,0) DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM bike_sales.sales_staging_2;

INSERT INTO bike_sales.sales_staging_2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY `Date`, `Day`, `Month`, `Year`, Customer_Age, Age_Group, Customer_Gender, Country, State, Product_Category, Sub_Category, Product_Description, Order_Quantity, Unit_Cost, Unit_Price, Profit, Cost, Revenue) AS row_num
FROM bike_sales.sales_staging;


# Let's identify the row(s) to be deleted

SELECT *
FROM bike_sales.sales_staging_2
WHERE row_num > 1;


# Let's delete the duplicate row(s)

DELETE
FROM bike_sales.sales_staging_2
WHERE row_num > 1;


# Let's check if the duplicate rows are deleted

SELECT *
FROM bike_sales.sales_staging_2
WHERE row_num > 1;


# The duplicate rows were successfully deleted, so now the table has only the rows where row_num = 1

SELECT *
FROM bike_sales.sales_staging_2;




# 3. Populate blank values
# I have identified two different blank values: one in the Age_Group column and the other in the Product_Description column
# The one on the Age_Group column is easily populated, since it is a category based on the Customer_Age column
# The one on the Product_Description column is not easily identifiable, so it's better to leave it as it is

# Let's identify the row with the blank value on the Age_Group

SELECT *
FROM bike_sales.sales_staging_2
WHERE Age_Group = '';


# Let's do a CASE Statement, in order to populate this

SELECT Sales_Order, Customer_Age, Age_Group,
CASE 
	WHEN Customer_Age < 25 THEN "Youth (<25)"
	WHEN Customer_Age >= 25 AND Customer_Age < 35 THEN "Young Adults (25-34)"
    WHEN Customer_Age >= 35 AND Customer_Age < 65 THEN "Adults (35-64)"
ELSE Age_Group
END AS Age_Group_New
FROM bike_sales.sales_staging_2;


# Now, let's update the Age_Group column, in order to populate the blank value

UPDATE bike_sales.sales_staging_2
SET Age_Group = 
CASE 
	WHEN Customer_Age < 25 THEN "Youth (<25)"
	WHEN Customer_Age >= 25 AND Customer_Age < 35 THEN "Young Adults (25-34)"
    WHEN Customer_Age >= 35 AND Customer_Age < 65 THEN "Adults (35-64)"
ELSE Age_Group
END;


# Let's check our table, to see if the blank value was successfully populated

SELECT *
FROM bike_sales.sales_staging_2;




# 4. Remove unnecessary columns
# So, in this step, I will proceed to delete unnecessary columns, such as Day, Month, Year and row_num 
# I will also delete the row in which there is a blank value for the column Product_Description, since it doesn't contain the full necessary information for future analysis and we are not able to populate it properly

# Let's identify the row with the blank value on the Product_Description and delete it

SELECT *
FROM bike_sales.sales_staging_2
WHERE Product_Description = '';

DELETE FROM bike_sales.sales_staging_2
WHERE Product_Description = '';


# Let's take a look at the full sales_staging_2 table again

SELECT *
FROM bike_sales.sales_staging_2;


# Let's delete the columns Day, Month, Year and row_num 

ALTER TABLE bike_sales.sales_staging_2
DROP COLUMN `Day`,
DROP COLUMN `Month`,
DROP COLUMN `Year`,
DROP COLUMN row_num;


# Checking again the sales_staging_2 table

SELECT *
FROM bike_sales.sales_staging_2;


# And now we have our dataset fully cleaned :)




