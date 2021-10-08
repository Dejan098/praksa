{{ config(materialized='table') }}
select
  "PassengerId",
  "Survived",
  "Pclass",
  "Name",
  "Sex",
  "Age",
  "SibSp",
  "Parch",
  "Ticket",
  "Fare",
  "Cabin",
  "Embarked"
from {{ source('imported_data', 'titanic') }}