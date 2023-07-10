# Simple SQL 


This project focuses on building a SQL compiler, which helps me understand how SQL work and also use C to do this. 

A breakdown of the model is the following: 

* Scanner
  + The parser enforces the syntax of the query: if the query is incorrect syntactically, then the parser will return an error. 
* Parser
  + Enforces semantic rules, or the meaning of the query. Do the actual inputs exist within a database?
* Analyzer
* Query Planner
* Execution
