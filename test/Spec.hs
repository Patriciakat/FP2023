import Data.Either
import Data.Maybe
import InMemoryTables qualified as D
import Lib1
import Lib2
import Test.Hspec
import Data.Aeson (encode, decode)
import Lib2 (DataFrame(..), Value(..))
import Lib3 (executeSql, runExecuteIO, initialInMemoryDB, serializeDataFrame, deserializeDataFrame)
import qualified Data.ByteString.Lazy as B
import System.IO (hClose)
import System.IO.Temp (withSystemTempFile)
import DataFrame (Column(..), ColumnType(..), DataFrame(..), Value(..))
import InMemoryTables (tableEmployees, tableInvalid1, tableInvalid2, tableLongStrings, tableWithNulls, tableDepartment)

main :: IO ()
main = hspec $ do

--Lib1.hs tests

  describe "Lib1.findTableByName" $ do
    it "handles empty lists" $ do
      Lib1.findTableByName [] "" `shouldBe` Nothing
    it "handles empty names" $ do
      Lib1.findTableByName D.database "" `shouldBe` Nothing
    it "can find by name" $ do
      Lib1.findTableByName D.database "employees" `shouldBe` Just (snd D.tableEmployees)
    it "can find by case-insensitive name" $ do
      Lib1.findTableByName D.database "employEEs" `shouldBe` Just (snd D.tableEmployees)
  describe "Lib1.parseSelectAllStatement" $ do
    it "handles empty input" $ do
      Lib1.parseSelectAllStatement "" `shouldSatisfy` isLeft
    it "handles invalid queries" $ do
      Lib1.parseSelectAllStatement "select from dual" `shouldSatisfy` isLeft
    it "returns table name from correct queries" $ do
      Lib1.parseSelectAllStatement "selecT * from dual;" `shouldBe` Right "dual"
  describe "Lib1.validateDataFrame" $ do
    it "finds types mismatch" $ do
      Lib1.validateDataFrame (snd D.tableInvalid1) `shouldSatisfy` isLeft
    it "finds column size mismatch" $ do
      Lib1.validateDataFrame (snd D.tableInvalid2) `shouldSatisfy` isLeft
    it "reports different error messages" $ do
      Lib1.validateDataFrame (snd D.tableInvalid1) `shouldNotBe` Lib1.validateDataFrame (snd D.tableInvalid2)
    it "passes valid tables" $ do
      Lib1.validateDataFrame (snd D.tableWithNulls) `shouldBe` Right ()
  describe "Lib1.renderDataFrameAsTable" $ do
    it "renders a table" $ do
      Lib1.renderDataFrameAsTable 100 (snd D.tableEmployees) `shouldSatisfy` not . null
      
--Lib2.hs query functionality tests setup with test DSL in lib3.hs file ran through Execute functions for inMemoryTables.hs rather than .json files.

  describe "Lib3.executeSql for SELECT queries" $ do
    let testDB = initialInMemoryDB
  
    it "selects all columns from employees" $ do
        result <- runExecuteIO True (executeSql "SELECT * FROM employees")  -- runExecuteIO with isTest set to True
        case result of
          Right df -> do
            df `shouldBe` DataFrame [Column "id" IntegerType, 
                                     Column "department_id" IntegerType, 
                                     Column "name" StringType, 
                                     Column "surname" StringType]
                                    [[IntegerValue 1, IntegerValue 100, StringValue "Vi", StringValue "Po"], 
                                     [IntegerValue 2, IntegerValue 101, StringValue "Ed", StringValue "Dl"],
                                     [IntegerValue 3, IntegerValue 102, StringValue "Ed", StringValue "Tr"],
                                     [IntegerValue 3, IntegerValue 101, StringValue "Ag", StringValue "Pt"]]
          Left errMsg -> fail errMsg
    
    it "selects id column from employees" $ do
        result <- runExecuteIO True (executeSql "SELECT id FROM employees")
        case result of
          Right df -> do
            df `shouldBe` DataFrame [Column "id" IntegerType]
                                    [[IntegerValue 1], 
                                     [IntegerValue 2], 
                                     [IntegerValue 3], 
                                     [IntegerValue 3]]
          Left errMsg -> fail errMsg

    it "selects id and name columns from employees" $ do
        result <- runExecuteIO True (executeSql "SELECT id, name FROM employees")
        case result of
          Right df -> do
            df `shouldBe` DataFrame [Column "id" IntegerType, 
                                     Column "name" StringType]
                                    [[IntegerValue 1, StringValue "Vi"], 
                                     [IntegerValue 2, StringValue "Ed"],
                                     [IntegerValue 3, StringValue "Ed"],
                                     [IntegerValue 3, StringValue "Ag"]]
          Left errMsg -> fail errMsg

    it "selects id, name, and surname columns from employees" $ do
        result <- runExecuteIO True (executeSql "SELECT id, name, surname FROM employees")
        case result of
          Right df -> do
            df `shouldBe` DataFrame [Column "id" IntegerType, 
                                     Column "name" StringType, 
                                     Column "surname" StringType]
                                    [[IntegerValue 1, StringValue "Vi", StringValue "Po"], 
                                     [IntegerValue 2, StringValue "Ed", StringValue "Dl"],
                                     [IntegerValue 3, StringValue "Ed", StringValue "Tr"],
                                     [IntegerValue 3, StringValue "Ag", StringValue "Pt"]]
          Left errMsg -> fail errMsg

  describe "Lib3.executeSql for MIN function queries" $ do
    let testDB = initialInMemoryDB

    it "selects min(id) from employees" $ do
      result <- runExecuteIO True (executeSql "SELECT MIN(id) FROM employees")
      case result of
        Right df -> df `shouldBe` DataFrame [Column "min(id)" IntegerType] [[IntegerValue 1]]
        Left errMsg -> fail errMsg

    it "selects min(id) from departments" $ do
      result <- runExecuteIO True (executeSql "SELECT MIN(id) FROM departments")
      case result of
        Right df -> df `shouldBe` DataFrame [Column "min(id)" IntegerType] [[IntegerValue 100]]
        Left errMsg -> fail errMsg

    it "selects min(id), name from employees" $ do
      result <- runExecuteIO True (executeSql "SELECT MIN(id), name FROM employees")
      case result of
        Right df -> df `shouldBe` DataFrame [Column "min(id)" IntegerType, Column "name" StringType] [[IntegerValue 1, StringValue "Vi"]]
        Left errMsg -> fail errMsg

    it "selects min(id), address, town from departments" $ do
      result <- runExecuteIO True (executeSql "SELECT MIN(id), address, town FROM departments")
      case result of
        Right df -> df `shouldBe` DataFrame [Column "min(id)" IntegerType, Column "address" StringType, Column "town" StringType] [[IntegerValue 100, StringValue "123 Market St.", StringValue "Townsville"]]
        Left errMsg -> fail errMsg

    it "selects min(id), department_id, name, surname from employees" $ do
      result <- runExecuteIO True (executeSql "SELECT MIN(id), department_id, name, surname FROM employees")
      case result of
        Right df -> df `shouldBe` DataFrame [Column "min(id)" IntegerType, Column "department_id" IntegerType, Column "name" StringType, Column "surname" StringType] [[IntegerValue 1, IntegerValue 100, StringValue "Vi", StringValue "Po"]]
        Left errMsg -> fail errMsg

  describe "Lib3.executeSql for AVG function queries" $ do
    let testDB = initialInMemoryDB

    it "selects avg(id) from employees" $ do
      result <- runExecuteIO True (executeSql "SELECT avg(id) FROM employees")
      case result of
        Right (DataFrame columns rows) -> do
          columns `shouldBe` [Column "avg(id)" FloatType]
          let expectedAvg = FloatValue $ (1 + 2 + 3 + 3) / 4
          rows `shouldBe` [[expectedAvg]]
        Left errMsg -> fail errMsg

    it "selects avg(id) from departments" $ do
      result <- runExecuteIO True (executeSql "SELECT avg(id) FROM departments")
      case result of
        Right (DataFrame columns rows) -> do
          columns `shouldBe` [Column "avg(id)" FloatType]
          let expectedAvg = FloatValue $ (100 + 101 + 102) / 3
          rows `shouldBe` [[expectedAvg]]
        Left errMsg -> fail errMsg

  describe "Lib3.executeSql for SHOW TABLE(S) queries" $ do
      let testDB = initialInMemoryDB

      it "shows all tables" $ do
          result <- runExecuteIO True (executeSql "SHOW TABLES")
          case result of
              Right df -> df `shouldBe` DataFrame [Column "Tables" StringType] 
                                                  [[StringValue "employees"], 
                                                   [StringValue "invalid1"], 
                                                   [StringValue "invalid2"], 
                                                   [StringValue "long_strings"], 
                                                   [StringValue "flags"], 
                                                   [StringValue "departments"]]
              Left errMsg -> fail errMsg

      it "shows schema of table employees" $ do
          result <- runExecuteIO True (executeSql "SHOW TABLE employees")
          case result of
              Right df -> df `shouldBe` DataFrame [Column "Column Name" StringType, Column "Type" StringType]
                                                  [[StringValue "id", StringValue "IntegerType"],
                                                   [StringValue "department_id", StringValue "IntegerType"],
                                                   [StringValue "name", StringValue "StringType"],
                                                   [StringValue "surname", StringValue "StringType"]]
              Left errMsg -> fail errMsg

      it "shows schema of table departments" $ do
          result <- runExecuteIO True (executeSql "SHOW TABLE departments")
          case result of
              Right df -> df `shouldBe` DataFrame [Column "Column Name" StringType, Column "Type" StringType]
                                                  [[StringValue "id", StringValue "IntegerType"],
                                                   [StringValue "name", StringValue "StringType"],
                                                   [StringValue "address", StringValue "StringType"],
                                                   [StringValue "town", StringValue "StringType"]]
              Left errMsg -> fail errMsg

      it "shows schema of table long_strings" $ do
              result <- runExecuteIO True (executeSql "SHOW TABLE long_strings")
              case result of
                  Right df -> df `shouldBe` DataFrame [Column "Column Name" StringType, Column "Type" StringType]
                                                      [[StringValue "text1", StringValue "StringType"],
                                                       [StringValue "text2", StringValue "StringType"]]
                  Left errMsg -> fail errMsg

      it "shows schema of table flags" $ do
              result <- runExecuteIO True (executeSql "SHOW TABLE flags")
              case result of
                  Right df -> df `shouldBe` DataFrame [Column "Column Name" StringType, Column "Type" StringType]
                                                      [[StringValue "flag", StringValue "StringType"],
                                                       [StringValue "value", StringValue "BoolType"]]
                  Left errMsg -> fail errMsg

      it "shows schema of table invalid1" $ do
              result <- runExecuteIO True (executeSql "SHOW TABLE invalid1")
              case result of
                  Right df -> df `shouldBe` DataFrame [Column "Column Name" StringType, Column "Type" StringType]
                                                      [[StringValue "id", StringValue "IntegerType"]]
                  Left errMsg -> fail errMsg

  describe "Lib3.executeSql for SELECT with WHERE queries" $ do
      let testDB = initialInMemoryDB

      it "selects * from employees where id=3" $ do
        result <- runExecuteIO True (executeSql "SELECT * FROM employees WHERE id=3")
        case result of
          Right (DataFrame columns rows) -> do
            columns `shouldBe` [Column "id" IntegerType, Column "department_id" IntegerType, Column "name" StringType, Column "surname" StringType]
            rows `shouldBe` [[IntegerValue 3, IntegerValue 102, StringValue "Ed", StringValue "Tr"],
                             [IntegerValue 3, IntegerValue 101, StringValue "Ag", StringValue "Pt"]]
          Left errMsg -> fail errMsg

      it "selects * from employees where id=3 and name=\"Ed\"" $ do
        result <- runExecuteIO True (executeSql "SELECT * FROM employees WHERE id=3 AND name=\"Ed\"")
        case result of
          Right (DataFrame columns rows) -> do
            columns `shouldBe` [Column "id" IntegerType, Column "department_id" IntegerType, Column "name" StringType, Column "surname" StringType]
            rows `shouldBe` [[IntegerValue 3, IntegerValue 102, StringValue "Ed", StringValue "Tr"]]
          Left errMsg -> fail errMsg

      it "selects * from employees where name=\"Ed\"" $ do
        result <- runExecuteIO True (executeSql "SELECT * FROM employees WHERE name=\"Ed\"")
        case result of
          Right (DataFrame columns rows) -> do
            columns `shouldBe` [Column "id" IntegerType, Column "department_id" IntegerType, Column "name" StringType, Column "surname" StringType]
            rows `shouldContain` [[IntegerValue 2, IntegerValue 101, StringValue "Ed", StringValue "Dl"]]
            rows `shouldContain` [[IntegerValue 3, IntegerValue 102, StringValue "Ed", StringValue "Tr"]]
          Left errMsg -> fail errMsg

      it "selects * from employees where id=3 and name=\"Ed\"" $ do
        result <- runExecuteIO True (executeSql "SELECT * FROM employees WHERE id=3 AND name=\"Ed\"")
        case result of
          Right (DataFrame columns rows) -> do
            columns `shouldBe` [Column "id" IntegerType, Column "department_id" IntegerType, Column "name" StringType, Column "surname" StringType]
            rows `shouldBe` [[IntegerValue 3, IntegerValue 102, StringValue "Ed", StringValue "Tr"]]
          Left errMsg -> fail errMsg

      it "selects id, name from employees where department_id=101" $ do
        result <- runExecuteIO True (executeSql "SELECT id, name FROM employees WHERE department_id=101")
        case result of
          Right (DataFrame columns rows) -> do
            columns `shouldBe` [Column "id" IntegerType, Column "name" StringType]
            rows `shouldBe` [[IntegerValue 2, StringValue "Ed"], [IntegerValue 3, StringValue "Ag"]]
          Left errMsg -> fail errMsg

      it "selects department_id, surname, name from employees where surname=\"Po\"" $ do
        result <- runExecuteIO True (executeSql "SELECT department_id, surname, name FROM employees WHERE surname=\"Po\"")
        case result of
          Right (DataFrame columns rows) -> do
            columns `shouldBe` [Column "department_id" IntegerType, Column "surname" StringType, Column "name" StringType]
            rows `shouldBe` [[IntegerValue 100, StringValue "Po", StringValue "Vi"]]
          Left errMsg -> fail errMsg

      it "selects id, town from departments where id=101" $ do
        result <- runExecuteIO True (executeSql "SELECT id, town FROM departments WHERE id=101")
        case result of
          Right (DataFrame columns rows) -> do
            columns `shouldBe` [Column "id" IntegerType, Column "town" StringType]
            rows `shouldBe` [[IntegerValue 101, StringValue "Moneytown"]]
          Left errMsg -> fail errMsg

      it "selects address, town from departments where id=102 and town=\"Techville\"" $ do
        result <- runExecuteIO True (executeSql "SELECT address, town FROM departments WHERE id=102 AND town=\"Techville\"")
        case result of
          Right (DataFrame columns rows) -> do
            columns `shouldBe` [Column "address" StringType, Column "town" StringType]
            rows `shouldBe` [[StringValue "789 Tech Blvd.", StringValue "Techville"]]
          Left errMsg -> fail errMsg
          
      it "serializes a DataFrame and writes to a file correctly" $ do
        let testDf = DataFrame [Column "id" IntegerType, Column "name" StringType]
                               [[IntegerValue 1, StringValue "Alice"], 
                                [IntegerValue 2, StringValue "Bob"]]
        let jsonContent = serializeDataFrame testDf
        withSystemTempFile "test.json" $ \filePath handle -> do
          B.hPut handle jsonContent
          hClose handle
          fileContent <- B.readFile filePath
          fileContent `shouldBe` jsonContent
          
  describe "Serialization and deserialization tests" $ do
          
      it "serializes a DataFrame and writes to a file correctly" $ do
              let testDf = DataFrame [Column "id" IntegerType, Column "name" StringType]
                                     [[IntegerValue 1, StringValue "Alice"], 
                                      [IntegerValue 2, StringValue "Bob"]]
              let jsonContent = serializeDataFrame testDf
              withSystemTempFile "test.json" $ \filePath handle -> do
                B.hPut handle jsonContent
                hClose handle
                fileContent <- B.readFile filePath
                fileContent `shouldBe` jsonContent
          
      it "deserializes a DataFrame from a file correctly" $ do
        let expectedDf = DataFrame [Column "age" IntegerType, Column "city" StringType]
                                   [[IntegerValue 30, StringValue "New York"], 
                                    [IntegerValue 25, StringValue "London"]]
        let jsonContent = serializeDataFrame expectedDf
        withSystemTempFile "test.json" $ \filePath handle -> do
          B.hPut handle jsonContent
          hClose handle
          fileContent <- B.readFile filePath
          let maybeDf = deserializeDataFrame fileContent
          isJust maybeDf `shouldBe` True
          fromJust maybeDf `shouldBe` expectedDf
          
      it "correctly serializes and deserializes a complex DataFrame" $ do
        let complexDf = DataFrame [Column "id" IntegerType, Column "name" StringType, Column "active" BoolType]
                                  [[IntegerValue 1, StringValue "Eve", BoolValue True], 
                                   [IntegerValue 2, StringValue "John", BoolValue False]]
        let jsonContent = serializeDataFrame complexDf
        withSystemTempFile "complex_test.json" $ \filePath handle -> do
          B.hPut handle jsonContent
          hClose handle
          fileContent <- B.readFile filePath
          let maybeDf = deserializeDataFrame fileContent
          isJust maybeDf `shouldBe` True
          fromJust maybeDf `shouldBe` complexDf