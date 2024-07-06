# Please add your team members' names here. 

## Team members' names 

1. Student Name: Nathaniel Li

   Student UT EID: nl9656


##  Course Name: CS378 - Cloud Computing 

##  Unique Number: 51515
    

Data cleaning:
We first check to see if it has 17 fields, then we parse the fields including the specified ones in the assignment. If any fields can't be parsed, then the whole entry is cleaned. We then further filter with travel time between 2 and 60 minutes, fare amount between 3 and 200, travel distance between 1 and 5, and toll amount being >= 3.0. If the pick-up time is empty, or the travel distance is <= 0, or the longitude/latitude of the pick-up/drop-off is 0, or the total amount is <= 0, the entry will also be cleaned.

# Add your Project REPORT HERE 

Task 1: 
Slope (m)	2.9843042781025315
Intercept (b)	0.3740792546528649
(screenshots attached)

Task 2:
m       2.952339046473074
b       0.8434964717468385
loss    20.47056804760588
(screenshots attached for first and last few iterations for brevity sake, but I can provide the middle iterations if needed)

Task 3:
weights {2.6210277793; 4.3359109701; 6.4241019147; 2.9510971819}
(screenshots attached)
