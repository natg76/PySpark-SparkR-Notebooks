

## get the source file into Master node using this command

# sudo wget http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/reviews_Electronics_5.json.gz
# sudo chmod 777 reviews_Electronics_5.json.gz
# gzip -d reviews_Electronics_5.json.gz



## Spark R Workbook - Data Analysis with Spark

# 1. Initialise a Spark session

# load SparkR
library(SparkR)

# initiating the spark session
sparkR.session(master='local')

# 2. Read the data file

data_electronics <- read.df("s3://ng-pgdss-in-1/ElectronicData/reviews_Electronics_5.json", source = "json", inferSchema = "true", header = "true")
nyc_2015 <- read.df("https://s3-us-west-2.amazonaws.com/hex-ml-trng/In-Datasets/nyc_2015_s.csv", source = "csv", inferSchema = "true", header = "true")

## data_electronics will be of class SparkDataFrame, not a regular R dataframe.

# 3. Examine data

head(data_electronics)
nrow(data_electronics)
ncol(data_electronics)
str(data_electronics)
printSchema(data_electronics)

# 4. Describe the data and collect the described file

df <- collect(describe(data_electronics))
df

# 5. Plotting data 


hist <- histogram(data_electronics, data_electronics$overall, nbins = 12)
class(hist)

## Above command creates a dataframe called "hist" , which is then used to plot using ggplot2.

library(ggplot2)
plot <- ggplot(hist, aes(x = centroids, y = counts)) +
  geom_bar(stat = "identity") +
  xlab("overall rating") + ylab("Frequency")

plot

# 6. Data Frame manipulation

# Select certain rows
head(select(data_electronics, data_electronics$summary))

# Apply a filter
head(filter(data_electronics, data_electronics$overall <= 4))
nrow(filter(data_electronics, data_electronics$overall <= 4))

# 7. Group by

# GroupBy
head(summarize(groupBy(data_electronics, data_electronics$overall),
               count = n(data_electronics$overall)))

#GroupBy + Sort
overall_counts <- summarize(groupBy(data_electronics, data_electronics$overall),
                            count = n(data_electronics$overall))
head(arrange(overall_counts, desc(overall_counts$count)))



# For using SQL, you need to create a temporary view
createOrReplaceTempView(data_electronics, "data_elec_tbl")


# Now, find the reviews which have a '5' rating
data_rated_5 <- SparkR::sql("SELECT * FROM data_elec_tbl WHERE overall = 5")
head(data_rated_5)
nrow(data_rated_5)


# First, create a length variable. This can be done in SQL very easily
data_withLength <- SparkR::sql("SELECT helpful, overall, reviewText, reviewTime, summary, asin, LENGTH(reviewText) AS reviewLength FROM data_elec_tbl")
createOrReplaceTempView(data_withLength, "data_elec_tbl")


# Binning into different lengths of reviewtext
bins <- sql("SELECT reviewLength, reviewText, asin, overall, helpful, \
                   CASE  WHEN reviewLength <= 1000  THEN 1\
                   WHEN (reviewLength > 1000  and reviewLength <= 2000) THEN 2\
                   WHEN (reviewLength > 2000 and reviewLength <= 3000) THEN 3\
                   WHEN (reviewLength > 3000 and reviewLength <= 4000) THEN 4\
                   ELSE 5 END  as bin_number FROM data_elec_tbl")

# Attach the bin number to the original DataFrame
data_electronics <- withColumn(data_electronics, "bin_number", lit("bins"))


# 8. Model Building
# Create length variable
data_withLength <- SparkR::sql("SELECT helpful, overall, reviewText, summary, asin, LENGTH(reviewText) AS reviewLength FROM data_elec_tbl")

createOrReplaceTempView(data_withLength, "data_elec_length_tbl")

# First create a dataframe for the model
data_electronics_formodel <- SparkR::sql("SELECT reviewLength, overall, helpful[0]/helpful[1] AS helpful_ratio 
FROM data_elec_length_tbl 
WHERE helpful[1] >= 10")

head(data_electronics_formodel)

# Build model
model_linear <- SparkR::glm(helpful_ratio ~ reviewLength, data = data_electronics_formodel, family = "gaussian")

# Create summary of model
summary_model_linear <- summary(model_linear)
summary_model_linear






# Build model
model_linear <- SparkR::glm(helpful_ratio ~ reviewLength, data = data_electronics_formodel, family = "gaussian")

# Create summary of model
summary_model_linear <- summary(model_linear)
summary_model_linear









## get the source file into Master node using this command

# sudo wget http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/reviews_Electronics_5.json.gz
# sudo chmod 777 reviews_Electronics_5.json.gz
# gzip -d reviews_Electronics_5.json.gz



## Spark R Workbook - Data Analysis with Spark

# 1. Initialise a Spark session

# load SparkR
library(SparkR)

# initiating the spark session
sparkR.session(master='local')

# 2. Read the data file

data_electronics <- read.df("s3://ng-pgdss-in-1/ElectronicData/reviews_Electronics_5.json", source = "json", inferSchema = "true", header = "true")

## data_electronics will be of class SparkDataFrame, not a regular R dataframe.

# 3. Examine data

head(data_electronics)
nrow(data_electronics)
ncol(data_electronics)
str(data_electronics)
printSchema(data_electronics)

de_small <-  head(data_electronics)

# 4. Describe the data and collect the described file

df <- collect(describe(data_electronics))
df

# 5. Plotting data 


hist <- histogram(data_electronics, data_electronics$overall, nbins = 12)
class(hist)

## Above command creates a dataframe called "hist" , which is then used to plot using ggplot2.

library(ggplot2)
plot <- ggplot(hist, aes(x = centroids, y = counts)) +
  geom_bar(stat = "identity") +
  xlab("overall rating") + ylab("Frequency")

plot

# 6. Data Frame manipulation

# Select certain rows
head(select(data_electronics, data_electronics$summary))

# Apply a filter
head(filter(data_electronics, data_electronics$overall <= 4))
nrow(filter(data_electronics, data_electronics$overall <= 4))



# 7. Group by

# GroupBy
head(summarize(groupBy(data_electronics, data_electronics$overall),
               count = n(data_electronics$overall)))

#GroupBy + Sort
overall_counts <- summarize(groupBy(data_electronics, data_electronics$overall),
                            count = n(data_electronics$overall))
head(arrange(overall_counts, desc(overall_counts$count)))



# For using SQL, you need to create a temporary view
createOrReplaceTempView(data_electronics, "data_elec_tbl")


# Now, find the reviews which have a '5' rating
data_rated_5 <- SparkR::sql("SELECT * FROM data_elec_tbl WHERE overall = 5")
head(data_rated_5)
nrow(data_rated_5)



# First, create a length variable. This can be done in SQL very easily
data_withLength <- SparkR::sql("SELECT helpful, overall, reviewText, reviewTime, summary, asin, LENGTH(reviewText) AS reviewLength FROM data_elec_tbl")
createOrReplaceTempView(data_withLength, "data_elec_tbl")


# Binning into different lengths of reviewtext
bins <- sql("SELECT reviewLength, reviewText, asin, overall, helpful, \
                   CASE  WHEN reviewLength <= 1000  THEN 1\
                   WHEN (reviewLength > 1000  and reviewLength <= 2000) THEN 2\
                   WHEN (reviewLength > 2000 and reviewLength <= 3000) THEN 3\
                   WHEN (reviewLength > 3000 and reviewLength <= 4000) THEN 4\
                   ELSE 5 END  as bin_number FROM data_elec_tbl")

# Attach the bin number to the original DataFrame
data_electronics <- withColumn(data_electronics, "bin_number", lit("bins"))
