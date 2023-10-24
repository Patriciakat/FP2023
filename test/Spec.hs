import Data.Either
import Data.Maybe ()
import InMemoryTables qualified as D
import Lib1
import Lib2
import Test.Hspec

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
      
--Lib2.hs tests
      
  describe "Lib2.parseStatement" $ do
    it "parses single column select" $ do
      Lib2.parseStatement "select id from employees" `shouldBe` Right (Lib2.SelectFrom ["id"] "employees")
  
    it "parses multiple column select" $ do
      Lib2.parseStatement "select id, surname from employees" `shouldBe` Right (Lib2.SelectFrom ["id", "surname"] "employees")
  
    it "parses select with conditions" $ do
      Lib2.parseStatement "select id, name from employees where id=1" `shouldBe` Right (Lib2.SelectWithConditions ["id", "name"] "employees" [EqualsCondition "id" (IntegerConditionValue 1)])
      
    it "handles SELECT * with uppercase" $ do
      Lib2.parseStatement "SELECT * FROM employees" `shouldBe` Right (Lib2.StatementSelectAll "employees")
            
    it "handles SELECT * with lowercase" $ do
      Lib2.parseStatement "select * from employees" `shouldBe` Right (Lib2.StatementSelectAll "employees")
    
    it "selects a single column" $ do
      Lib2.parseStatement "select text1 from long_strings" `shouldBe` Right (Lib2.SelectFrom ["text1"] "long_strings")
    
    it "selects multiple columns" $ do
      Lib2.parseStatement "select text1, text2 from long_strings" `shouldBe` Right (Lib2.SelectFrom ["text1", "text2"] "long_strings")
    
    it "handles MIN function with uppercase" $ do
      Lib2.parseStatement "select MIN(id) from employees" `shouldBe` Right (Lib2.SelectMin ["id"] "employees")
    
    it "handles MIN function with lowercase" $ do
      Lib2.parseStatement "select min(id) from employees" `shouldBe` Right (Lib2.SelectMin ["id"] "employees")
    
    it "handles MIN function with other columns" $ do
      Lib2.parseStatement "select min(id), name from employees" `shouldBe` Right (Lib2.SelectWithMin ["id"] ["name"] "employees")
    
    it "handles AVG function with uppercase" $ do
      Lib2.parseStatement "select AVG(id) from employees" `shouldBe` Right (Lib2.SelectAvg ["id"] "employees")
    
    it "handles AVG function with lowercase" $ do
      Lib2.parseStatement "select avg(id) from employees" `shouldBe` Right (Lib2.SelectAvg ["id"] "employees")
    
    it "handles WHERE condition with single condition" $ do
      Lib2.parseStatement "select id from employees where id=3" `shouldBe` Right (Lib2.SelectWithConditions ["id"] "employees" [Lib2.EqualsCondition "id" (Lib2.IntegerConditionValue 3)])
    
    it "handles WHERE condition with AND clause" $ do
      Lib2.parseStatement "select id from employees where id=3 and name=\"Ag\"" `shouldBe` Right (Lib2.SelectWithConditions ["id"] "employees" [Lib2.EqualsCondition "id" (Lib2.IntegerConditionValue 3), Lib2.EqualsCondition "name" (Lib2.StringConditionValue "Ag")])
      
    it "select with WHERE and single condition on multiple columns" $ do
        Lib2.parseStatement "select id, surname from employees where id=3" `shouldBe` Right (Lib2.SelectWithConditions ["id", "surname"] "employees" [Lib2.EqualsCondition "id" (Lib2.IntegerConditionValue 3)])
    
    it "select with WHERE and AND condition on multiple columns" $ do
        Lib2.parseStatement "select id, surname from employees where id=3 and name=\"Ag\"" `shouldBe` Right (Lib2.SelectWithConditions ["id", "surname"] "employees" [Lib2.EqualsCondition "id" (Lib2.IntegerConditionValue 3), Lib2.EqualsCondition "name" (Lib2.StringConditionValue "Ag")])
    
    it "select with multiple WHERE and AND conditions on multiple columns" $ do
        Lib2.parseStatement "select id, surname, name from employees where id=3 and name=\"Ag\" and surname=\"Pt\"" `shouldBe` Right (Lib2.SelectWithConditions ["id", "surname", "name"] "employees" [Lib2.EqualsCondition "id" (Lib2.IntegerConditionValue 3), Lib2.EqualsCondition "name" (Lib2.StringConditionValue "Ag"), Lib2.EqualsCondition "surname" (Lib2.StringConditionValue "Pt")])
    
    it "select MIN with WHERE condition" $ do
        Lib2.parseStatement "select min(id) from employees where name=\"Ed\"" `shouldBe` Right (Lib2.SelectMin ["id"] "employees") -- You might need additional logic in `executeStatement` to handle this case.
    
    it "select MIN with other columns and WHERE condition" $ do
        Lib2.parseStatement "select min(id), name, surname from employees where name=\"Ed\"" `shouldBe` Right (Lib2.SelectWithMin ["id"] ["name", "surname"] "employees")
        
    -- Invalid cases
    
    it "select from a non-existent table" $ do
        let statement = Lib2.parseStatement "select * from non_existent_table"
        case statement of
            Right stmt -> Lib2.executeStatement stmt D.database `shouldSatisfy` isLeft
            _ -> fail "Parsing failed."
    
    it "select a column with non-matching condition" $ do
        Lib2.parseStatement "select name from employees where id=9999" `shouldBe` Right (Lib2.SelectWithConditions ["name"] "employees" [Lib2.EqualsCondition "id" (Lib2.IntegerConditionValue 9999)])
    
    it "select MIN on a non-integer column" $ do
        Lib2.parseStatement "select min(surname) from employees" `shouldBe` Right (Lib2.SelectMin ["surname"] "employees")
    
    it "select from another non-existent table" $ do
        let statement = Lib2.parseStatement "select text1 from non_existent_table"
        case statement of
            Right stmt -> Lib2.executeStatement stmt D.database `shouldSatisfy` isLeft
            _ -> fail "Parsing failed."
    
    it "malformed select with extra comma" $ do
        Lib2.parseStatement "select text1, from long_strings" `shouldNotBe` Right (Lib2.SelectFrom ["text1"] "long_strings")
    
    it "select with non-matching WHERE condition" $ do
        Lib2.parseStatement "select id, surname from employees where name=\"nonExistingName\"" `shouldBe` Right (Lib2.SelectWithConditions ["id", "surname"] "employees" [Lib2.EqualsCondition "name" (Lib2.StringConditionValue "nonExistingName")])
    
    it "select with single quotes instead of double quotes" $ do
        Lib2.parseStatement "select id, surname from employees where name='Vi'" `shouldNotBe` Right (Lib2.SelectWithConditions ["id", "surname"] "employees" [Lib2.EqualsCondition "name" (Lib2.StringConditionValue "Vi")])    