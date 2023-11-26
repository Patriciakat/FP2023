import Data.Either
import Data.Maybe
import InMemoryTables qualified as D
import Lib1
import Lib2
import Test.Hspec
import Lib2 (DataFrame(..), Value(..))
import Lib3 (executeSql, runTestExecuteIO, initialInMemoryDB)
import DataFrame (Column(..), ColumnType(..))
--Lib1.hs tests

main :: IO ()
main = hspec $ do
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
      
--Lib2.hs query test setup with test DSL in lib3.hs file

  describe "Lib3.executeSql for SELECT queries" $ do
    let testDB = initialInMemoryDB
  
    it "selects all columns from employees" $ do
        let (result, _) = runTestExecuteIO (executeSql True "SELECT * FROM employees") initialInMemoryDB
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
        let (result, _) = runTestExecuteIO (executeSql True "SELECT id FROM employees") initialInMemoryDB
        case result of
          Right df -> do
            df `shouldBe` DataFrame [Column "id" IntegerType]
                                    [[IntegerValue 1], 
                                     [IntegerValue 2], 
                                     [IntegerValue 3], 
                                     [IntegerValue 3]]
          Left errMsg -> fail errMsg
    
    it "selects id and name columns from employees" $ do
        let (result, _) = runTestExecuteIO (executeSql True "SELECT id, name FROM employees") initialInMemoryDB
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
        let (result, _) = runTestExecuteIO (executeSql True "SELECT id, name, surname FROM employees") initialInMemoryDB
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
      let (result, _) = runTestExecuteIO (executeSql True "SELECT MIN(id) FROM employees") testDB
      case result of
        Right df -> df `shouldBe` DataFrame [Column "min(id)" IntegerType] [[IntegerValue 1]]
        Left errMsg -> fail errMsg
  
    it "selects min(id) from departments" $ do
      let (result, _) = runTestExecuteIO (executeSql True "SELECT MIN(id) FROM departments") testDB
      case result of
        Right df -> df `shouldBe` DataFrame [Column "min(id)" IntegerType] [[IntegerValue 100]]
        Left errMsg -> fail errMsg
  
    it "selects min(id), name from employees" $ do
      let (result, _) = runTestExecuteIO (executeSql True "SELECT MIN(id), name FROM employees") testDB
      case result of
        Right df -> df `shouldBe` DataFrame [Column "min(id)" IntegerType, Column "name" StringType] [[IntegerValue 1, StringValue "Vi"]]
        Left errMsg -> fail errMsg
  
    it "selects min(id), address, town from departments" $ do
      let (result, _) = runTestExecuteIO (executeSql True "SELECT MIN(id), address, town FROM departments") testDB
      case result of
        Right df -> df `shouldBe` DataFrame [Column "min(id)" IntegerType, Column "address" StringType, Column "town" StringType] [[IntegerValue 100, StringValue "123 Market St.", StringValue "Townsville"]]
        Left errMsg -> fail errMsg
  
    it "selects min(id), department_id, name, surname from employees" $ do
      let (result, _) = runTestExecuteIO (executeSql True "SELECT MIN(id), department_id, name, surname FROM employees") testDB
      case result of
        Right df -> df `shouldBe` DataFrame [Column "min(id)" IntegerType, Column "department_id" IntegerType, Column "name" StringType, Column "surname" StringType] [[IntegerValue 1, IntegerValue 100, StringValue "Vi", StringValue "Po"]]
        Left errMsg -> fail errMsg
        
  describe "Lib3.executeSql for AVG function queries" $ do
    let testDB = initialInMemoryDB
  
    it "selects avg(id) from employees" $ do
      let (result, _) = runTestExecuteIO (executeSql True "SELECT avg(id) FROM employees") testDB
      case result of
        Right (DataFrame columns rows) -> do
          columns `shouldBe` [Column "avg(id)" FloatType]
          let expectedAvg = FloatValue $ (1 + 2 + 3 + 3) / 4
          rows `shouldBe` [[expectedAvg]]
        Left errMsg -> fail errMsg
  
    it "selects avg(id) from departments" $ do
      let (result, _) = runTestExecuteIO (executeSql True "SELECT avg(id) FROM departments") testDB
      case result of
        Right (DataFrame columns rows) -> do
          columns `shouldBe` [Column "avg(id)" FloatType]
          let expectedAvg = FloatValue $ (100 + 101 + 102) / 3
          rows `shouldBe` [[expectedAvg]]
        Left errMsg -> fail errMsg
        
  describe "Lib3.executeSql for SHOW TABLE(S) queries" $ do
      let testDB = initialInMemoryDB
  
      it "shows all tables" $ do
          let (result, _) = runTestExecuteIO (executeSql True "SHOW TABLES") testDB
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
          let (result, _) = runTestExecuteIO (executeSql True "SHOW TABLE employees") testDB
          case result of
              Right df -> df `shouldBe` DataFrame [Column "Column Name" StringType, Column "Type" StringType]
                                                  [[StringValue "id", StringValue "IntegerType"],
                                                   [StringValue "department_id", StringValue "IntegerType"],
                                                   [StringValue "name", StringValue "StringType"],
                                                   [StringValue "surname", StringValue "StringType"]]
              Left errMsg -> fail errMsg
  
      it "shows schema of table departments" $ do
          let (result, _) = runTestExecuteIO (executeSql True "SHOW TABLE departments") testDB
          case result of
              Right df -> df `shouldBe` DataFrame [Column "Column Name" StringType, Column "Type" StringType]
                                                  [[StringValue "id", StringValue "IntegerType"],
                                                   [StringValue "name", StringValue "StringType"],
                                                   [StringValue "address", StringValue "StringType"],
                                                   [StringValue "town", StringValue "StringType"]]
              Left errMsg -> fail errMsg
              
      it "shows schema of table long_strings" $ do
              let (result, _) = runTestExecuteIO (executeSql True "SHOW TABLE long_strings") testDB
              case result of
                  Right df -> df `shouldBe` DataFrame [Column "Column Name" StringType, Column "Type" StringType]
                                                      [[StringValue "text1", StringValue "StringType"],
                                                       [StringValue "text2", StringValue "StringType"]]
                  Left errMsg -> fail errMsg
      
      it "shows schema of table flags" $ do
              let (result, _) = runTestExecuteIO (executeSql True "SHOW TABLE flags") testDB
              case result of
                  Right df -> df `shouldBe` DataFrame [Column "Column Name" StringType, Column "Type" StringType]
                                                      [[StringValue "flag", StringValue "StringType"],
                                                       [StringValue "value", StringValue "BoolType"]]
                  Left errMsg -> fail errMsg
              
      it "shows schema of table invalid1" $ do
              let (result, _) = runTestExecuteIO (executeSql True "SHOW TABLE invalid1") testDB
              case result of
                  Right df -> df `shouldBe` DataFrame [Column "Column Name" StringType, Column "Type" StringType]
                                                      [[StringValue "id", StringValue "IntegerType"]]
                  Left errMsg -> fail errMsg
                  
  describe "Lib3.executeSql for SELECT with WHERE queries" $ do
      let testDB = initialInMemoryDB
    
      it "selects * from employees where id=3" $ do
        let (result, _) = runTestExecuteIO (executeSql True "SELECT * FROM employees WHERE id=3") testDB
        case result of
          Right (DataFrame columns rows) -> do
            columns `shouldBe` [Column "id" IntegerType, Column "department_id" IntegerType, Column "name" StringType, Column "surname" StringType]
            rows `shouldBe` [[IntegerValue 3, IntegerValue 102, StringValue "Ed", StringValue "Tr"],
                             [IntegerValue 3, IntegerValue 101, StringValue "Ag", StringValue "Pt"]]
          Left errMsg -> fail errMsg
  
      it "selects * from employees where id=3 and name=\"Ed\"" $ do
        let (result, _) = runTestExecuteIO (executeSql True "SELECT * FROM employees WHERE id=3 AND name=\"Ed\"") testDB
        case result of
          Right (DataFrame columns rows) -> do
            columns `shouldBe` [Column "id" IntegerType, Column "department_id" IntegerType, Column "name" StringType, Column "surname" StringType]
            rows `shouldBe` [[IntegerValue 3, IntegerValue 102, StringValue "Ed", StringValue "Tr"]]
          Left errMsg -> fail errMsg
  
      it "selects * from employees where name=\"Ed\"" $ do
        let (result, _) = runTestExecuteIO (executeSql True "SELECT * FROM employees WHERE name=\"Ed\"") testDB
        case result of
          Right (DataFrame columns rows) -> do
            columns `shouldBe` [Column "id" IntegerType, Column "department_id" IntegerType, Column "name" StringType, Column "surname" StringType]
            rows `shouldContain` [[IntegerValue 2, IntegerValue 101, StringValue "Ed", StringValue "Dl"]]
            rows `shouldContain` [[IntegerValue 3, IntegerValue 102, StringValue "Ed", StringValue "Tr"]]
          Left errMsg -> fail errMsg

      it "selects * from employees where id=3 and name=\"Ed\"" $ do
        let (result, _) = runTestExecuteIO (executeSql True "SELECT * FROM employees WHERE id=3 AND name=\"Ed\"") testDB
        case result of
          Right (DataFrame columns rows) -> do
            columns `shouldBe` [Column "id" IntegerType, Column "department_id" IntegerType, Column "name" StringType, Column "surname" StringType]
            rows `shouldBe` [[IntegerValue 3, IntegerValue 102, StringValue "Ed", StringValue "Tr"]]
          Left errMsg -> fail errMsg
          
      it "selects id, name from employees where department_id=101" $ do
        let (result, _) = runTestExecuteIO (executeSql True "SELECT id, name FROM employees WHERE department_id=101") testDB
        case result of
          Right (DataFrame columns rows) -> do
            columns `shouldBe` [Column "id" IntegerType, Column "name" StringType]
            rows `shouldBe` [[IntegerValue 2, StringValue "Ed"], [IntegerValue 3, StringValue "Ag"]]
          Left errMsg -> fail errMsg
          
      it "selects department_id, surname, name from employees where surname=\"Po\"" $ do
        let (result, _) = runTestExecuteIO (executeSql True "SELECT department_id, surname, name FROM employees WHERE surname=\"Po\"") testDB
        case result of
          Right (DataFrame columns rows) -> do
            columns `shouldBe` [Column "department_id" IntegerType, Column "surname" StringType, Column "name" StringType]
            rows `shouldBe` [[IntegerValue 100, StringValue "Po", StringValue "Vi"]]
          Left errMsg -> fail errMsg
          
      it "selects id, town from departments where id=101" $ do
        let (result, _) = runTestExecuteIO (executeSql True "SELECT id, town FROM departments WHERE id=101") testDB
        case result of
          Right (DataFrame columns rows) -> do
            columns `shouldBe` [Column "id" IntegerType, Column "town" StringType]
            rows `shouldBe` [[IntegerValue 101, StringValue "Moneytown"]]
          Left errMsg -> fail errMsg
  
      it "selects address, town from departments where id=102 and town=\"Techville\"" $ do
        let (result, _) = runTestExecuteIO (executeSql True "SELECT address, town FROM departments WHERE id=102 AND town=\"Techville\"") testDB
        case result of
          Right (DataFrame columns rows) -> do
            columns `shouldBe` [Column "address" StringType, Column "town" StringType]
            rows `shouldBe` [[StringValue "789 Tech Blvd.", StringValue "Techville"]]
          Left errMsg -> fail errMsg