-- Exploratory Data Analysis

-- Dataset from: https://www.kaggle.com/datasets/ratnarohith/uncleaned-bike-sales-data

-- Skills used: CTE's, Dense Rank, Substrings, Partition By



# Checking everything on the sales_staging_2 table

SELECT *
FROM bike_sales.sales_staging_2;


# Let's start with some easy queries
# Let's explore the Revenue and Profit to see how big these bike sales were

SELECT MAX(Revenue), MIN(Revenue), MAX(Profit), MIN(Profit)
FROM bike_sales.sales_staging_2;


# How many distinct bikes do we have in this table

SELECT COUNT(DISTINCT Product_Description) AS `Count`
FROM bike_sales.sales_staging_2;


# How many bikes have been sold and their respective name

SELECT Product_Description, SUM(Order_Quantity) AS Quantity
FROM bike_sales.sales_staging_2
GROUP BY Product_Description
ORDER BY 2 DESC;


# Bikes with the biggest single profit (limit 5)

SELECT Product_Description AS Bikes, Profit
FROM bike_sales.sales_staging_2
ORDER BY 2 DESC, 1 ASC
LIMIT 5;


# Bikes with the smallest single profit (limit 5)

SELECT Product_Description AS Bikes, Profit
FROM bike_sales.sales_staging_2
ORDER BY 2 ASC, 1 ASC
LIMIT 5;


# Top 5 bikes with the most total profit
# It's not necessary to multiply the Profit by the Order_Quantity, since the Profit column is the calculated difference between the Revenue and the Cost (which are considering the Quantities already)

SELECT Product_Description, SUM(Profit)
FROM bike_sales.sales_staging_2
GROUP BY Product_Description
ORDER BY 2 DESC
LIMIT 5;


# Same but by State, instead of by bike

SELECT State, SUM(Profit)
FROM bike_sales.sales_staging_2
GROUP BY State
ORDER BY 2 DESC
LIMIT 5;


# Now I've got curious about the Country. Let's check both the State and the Country

SELECT Country, State, SUM(Profit)
FROM bike_sales.sales_staging_2
GROUP BY Country, State
ORDER BY 3 DESC
LIMIT 5;


# Just double checking the different countries that we have in this table

SELECT DISTINCT Country
FROM bike_sales.sales_staging_2;


# How many female and male buyers and the total revenue for each

SELECT Customer_Gender, COUNT(Customer_Gender) AS Count_Gender, SUM(Revenue) AS Total_Revenue
FROM bike_sales.sales_staging_2
GROUP BY Customer_Gender;


# Double checking the Overall Total Revenue, to see if the previous result is correct

SELECT SUM(Revenue)
FROM bike_sales.sales_staging_2;


# Which Age Group is buying more bikes

SELECT Age_Group, SUM(Order_Quantity) AS Quantity
FROM bike_sales.sales_staging_2
GROUP BY Age_Group
ORDER BY 2 DESC;


# Days with the biggest total revenue and quantity (limit 5)
# Using SUBSTRING() to just pickup the Days, since the table only contains days of December 2021

SELECT SUBSTRING(`Date`, 1, 2) AS `Day`, SUM(Revenue) AS Total_Revenue, SUM(Order_Quantity) AS Total_Quantity
FROM bike_sales.sales_staging_2
GROUP BY `Day`
ORDER BY Total_Revenue DESC, Total_Quantity DESC
LIMIT 5;


# Rolling total of Profit, Cost and Revenue per Day
# Let's start with the query that gives us the total per day

SELECT SUBSTRING(`Date`, 1, 2) AS `Day`, SUM(Profit) AS Total_Profit, SUM(Cost) AS Total_Cost, SUM(Revenue) AS Total_Revenue
FROM bike_sales.sales_staging_2
GROUP BY `Day`
ORDER BY `Day` ASC;


# Now, let's do a CTE to perform the Rolling total

WITH Day_CTE AS
(
SELECT SUBSTRING(`Date`, 1, 2) AS `Day`, SUM(Profit) AS Total_Profit, SUM(Cost) AS Total_Cost, SUM(Revenue) AS Total_Revenue
FROM bike_sales.sales_staging_2
GROUP BY `Day`
ORDER BY `Day` ASC
)
SELECT `Day`, SUM(Total_Profit) OVER (ORDER BY `Day` ASC) AS Rolling_Total_Profit, 
	SUM(Total_Cost) OVER (ORDER BY `Day` ASC) AS Rolling_Total_Cost,
    SUM(Total_Revenue) OVER (ORDER BY `Day` ASC) AS Rolling_Total_Revenue
FROM Day_CTE
ORDER BY `Day` ASC;


# Let's bring in also the Total_Profit, Total_Cost and Total_Revenue to see if the Rolling Totals are behaving as expected

WITH Day_CTE AS
(
SELECT SUBSTRING(`Date`, 1, 2) AS `Day`, SUM(Profit) AS Total_Profit, SUM(Cost) AS Total_Cost, SUM(Revenue) AS Total_Revenue
FROM bike_sales.sales_staging_2
GROUP BY `Day`
ORDER BY `Day` ASC
)
SELECT `Day`, 
	Total_Profit, SUM(Total_Profit) OVER (ORDER BY `Day` ASC) AS Rolling_Total_Profit, 
	Total_Cost, SUM(Total_Cost) OVER (ORDER BY `Day` ASC) AS Rolling_Total_Cost,
    Total_Revenue, SUM(Total_Revenue) OVER (ORDER BY `Day` ASC) AS Rolling_Total_Revenue
FROM Day_CTE
ORDER BY `Day` ASC;


# Top 3 of the most sold Bikes per Country and their respective Total Quantity and Total Revenue

# Let's do one CTE to get the totals per Country and per Bike
# Then, let's do another CTE to get the Ranking of the Bikes with the highest Revenue per Country
# And finally let's filter the Ranking, in order to get the Top 3 of the most sold Bikes per Country

# We need to do two CTE's instead of one, otherwise we cannot perform a filter on Ranking

WITH Bike_Revenue AS 
(
SELECT Country, Product_Description, SUM(Revenue) AS Total_Revenue, SUM(Order_Quantity) AS Total_Quantity
FROM bike_sales.sales_staging_2
GROUP BY Country, Product_Description
)
,
Bike_Revenue_Rank AS 
(
SELECT Country, Product_Description, Total_Revenue, Total_Quantity, DENSE_RANK() OVER (PARTITION BY Country ORDER BY Total_Revenue DESC) AS Ranking
FROM Bike_Revenue
)
SELECT Country, Product_Description, Total_Revenue, Total_Quantity, Ranking
FROM Bike_Revenue_Rank
WHERE Ranking <= 3
ORDER BY Country ASC, Total_Revenue DESC;


# And that's it for the Data Exploration on this dataset :)




