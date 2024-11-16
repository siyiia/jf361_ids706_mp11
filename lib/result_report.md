
## Iris Data Extracted
|    |   sepal_length |   sepal_width |   petal_length |   petal_width | species   |
|---:|---------------:|--------------:|---------------:|--------------:|:----------|
|  0 |            5.1 |           3.5 |            1.4 |           0.2 | setosa    |
|  1 |            4.9 |           3   |            1.4 |           0.2 | setosa    |
|  2 |            4.7 |           3.2 |            1.3 |           0.2 | setosa    |
|  3 |            4.6 |           3.1 |            1.5 |           0.2 | setosa    |
|  4 |            5   |           3.6 |            1.4 |           0.2 | setosa    |
|  5 |            5.4 |           3.9 |            1.7 |           0.4 | setosa    |
|  6 |            4.6 |           3.4 |            1.4 |           0.3 | setosa    |
|  7 |            5   |           3.4 |            1.5 |           0.2 | setosa    |
|  8 |            4.4 |           2.9 |            1.4 |           0.2 | setosa    |
|  9 |            4.9 |           3.1 |            1.5 |           0.1 | setosa    |

## Summary of Iris Data Transformations
|    | species    |   AvgPetalLength |   AvgSepalWidth |   AvgPetalToSepalRatio |
|---:|:-----------|-----------------:|----------------:|-----------------------:|
|  0 | virginica  |             5.55 |            2.97 |                   0.84 |
|  1 | versicolor |             4.26 |            2.77 |                   0.72 |
|  2 | setosa     |             1.46 |            3.43 |                   0.29 |

## Query Result for Table: jf361_iris_table
### Query:
```
SELECT * FROM jf361_iris_table LIMIT 10
```

|    |   SepalLength |   SepalWidth |   PetalLength |   PetalWidth | species   |   PetalToSepalRatio | PetalCategory   |
|---:|--------------:|-------------:|--------------:|-------------:|:----------|--------------------:|:----------------|
|  0 |           5.1 |          3.5 |           1.4 |          0.2 | setosa    |                0.27 | Small           |
|  1 |           4.9 |          3   |           1.4 |          0.2 | setosa    |                0.29 | Small           |
|  2 |           4.7 |          3.2 |           1.3 |          0.2 | setosa    |                0.28 | Small           |
|  3 |           4.6 |          3.1 |           1.5 |          0.2 | setosa    |                0.33 | Small           |
|  4 |           5   |          3.6 |           1.4 |          0.2 | setosa    |                0.28 | Small           |
|  5 |           5.4 |          3.9 |           1.7 |          0.4 | setosa    |                0.31 | Small           |
|  6 |           4.6 |          3.4 |           1.4 |          0.3 | setosa    |                0.3  | Small           |
|  7 |           5   |          3.4 |           1.5 |          0.2 | setosa    |                0.3  | Small           |
|  8 |           4.4 |          2.9 |           1.4 |          0.2 | setosa    |                0.32 | Small           |
|  9 |           4.9 |          3.1 |           1.5 |          0.1 | setosa    |                0.31 | Small           |
