### Project 1
A CLI application to process response data from News API and answer 6 analytical questions. User login information is stored within Hive and the passwords are encrypted. 
- API Source: https://newsapi.org/

### Requirements
- ALL user interaction must come purely from the console application
- Hive/MapReduce must:
    - scrap data from datasets from an API based on your topic of choice
- Your console application must:
    - query data to answer at least 6 analysis questions
    - have a login system for all users with passwords
        - 2 types of users: BASIC and ADMIN
        - Users should also be able to update username and password

### Analysis questions for Project
1. What is the top keyword related to gaming?
2. What news source talks about gaming the most?
3. What is the most mentioned console?
4. What is the most popular game?
5. What is the most popular game publisher?
6. What percent of all top news headlines is gaming related?

### Technologies
- Hadoop MapReduce
- YARN
- HDFS
- Scala 2.13
- Hive
- Git + GitHub
