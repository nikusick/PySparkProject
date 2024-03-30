from pyspark.sql import SparkSession
import findspark
findspark.init()

spark = SparkSession.builder.getOrCreate()

categories = spark.createDataFrame(
    [
        (0, 'vegetables'),
        (1, 'fruits'),
        (2, 'food')
    ],
    [
        'id',
        'name'
    ]
)

products = spark.createDataFrame(
    [
        (0, 'apple'),
        (1, 'tomato'),
        (2, 'shoes')
    ],
    [
        'id',
        'name'
    ]
)

products_categories = spark.createDataFrame(
    [
        (0, 1),
        (0, 2),
        (1, 0),
        (1, 2),
    ],
    [
        'product_id',
        'category_id'
    ]
)

# Реализованный метод
result = products.\
    join(products_categories, products_categories.product_id == products.id, 'outer').\
    join(categories, products_categories.category_id == categories.id, 'outer').\
    select(products.name.alias('product_name'), categories.name.alias('category_name')).\
    sort('product_name', 'category_name')
with_cat = result.filter(result.category_name.isNotNull())
no_cat = result.filter(result.category_name.isNull())

result.show()
with_cat.show()
no_cat.show()
