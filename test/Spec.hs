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

