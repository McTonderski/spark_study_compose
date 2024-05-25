##### a

a <- 4 * sin(pi)
b <- 3 * a

compare_variables <- function(a, b) {
  if (a > b) {
    return("Variable 'a' is greater.")
  } else if (b > a) {
    return("Variable 'b' is greater.")
  } else {
    return("Both variables are equal.")
  }
}

result <- compare_variables(a, b)
cat(result, "\n")


##### b
help_max <- help(max)
cat(help_max)



##### c
a <- 90:115
average_squared <- mean(a^2)
cat("Average of squared numbers:", average_squared, "\n")


##### d
functions_with_max <- ls("package:base", pattern = "max")
cat("Functions containing 'max' in their name:", functions_with_max, "\n")

##### e
# Set the working directory
setwd("/home/r-environment")

# Create and save a variable 'a' to a file
a <- "lodowka z najwieksza pojemnoscia"
save(a, file = "variable_a.Rdata")

# Delete the variable 'a'
rm(a)

# Check if variable 'a' exists (it should be deleted)
if (exists("a")) {
  cat("Variable 'a' exists.\n")
} else {
  cat("Variable 'a' does not exist.\n")
}

# Load the variable 'a' from the file
load("variable_a.Rdata")
cat("Loaded variable 'a':", a, "\n")

#### f
# Install and load the 'gridExtra' package
install.packages("gridExtra")
library(gridExtra)

# Find a function for visualizing data tables
?grid.table