module Main (main) where

import Control.Monad.IO.Class (MonadIO (liftIO))
import Control.Monad.Free (Free (..))
import Data.Functor ((<&>))
import Data.Time (UTCTime, getCurrentTime)
import Data.List qualified as L
import System.Console.Repline
import System.Console.Terminal.Size
import qualified Data.ByteString.Lazy as B

import Lib1 qualified
import Lib2 qualified
import Lib3 qualified
import Lib3 (serializeDataFrame, Execution, executeSql)
import DataFrame
import InMemoryTables (tableEmployees, tableInvalid1, tableInvalid2, tableLongStrings, tableWithNulls)

type Repl a = HaskelineT IO a

final :: Repl ExitDecision
final = do
  liftIO $ putStrLn "Goodbye!"
  return Exit

ini :: Repl ()
ini = liftIO $ putStrLn "Welcome to select-manipulate database! Press [TAB] for auto completion."

completer :: (Monad m) => WordCompleter m
completer n = do
  let names = [
              "select", "*", "from", "show", "table",
              "tables", "insert", "into", "values",
              "set", "update", "delete"
              ]
  return $ Prelude.filter (L.isPrefixOf n) names

-- Evaluation : handle each line user inputs
cmd :: String -> Repl ()
cmd c = do
  s <- terminalWidth <$> liftIO size
  result <- liftIO $ cmd' s
  case result of
    Left err -> liftIO $ putStrLn $ "Error: " ++ err
    Right table -> liftIO $ putStrLn table
  where
    terminalWidth :: (Integral n) => Maybe (Window n) -> n
    terminalWidth = maybe 80 width
    cmd' :: Integer -> IO (Either String String)
    cmd' s = do
      df <- runExecuteIO $ Lib3.executeSql c 
      return $ Lib1.renderDataFrameAsTable s <$> df

-- Function to serialize all tables on startup
serializeTables :: IO ()
serializeTables = do
  let tables = [tableEmployees, tableInvalid1, tableInvalid2, tableLongStrings, tableWithNulls]
  mapM_ (\(name, df) -> B.writeFile ("db/" ++ name ++ ".json") (serializeDataFrame df)) tables

main :: IO ()
main = do
  serializeTables  -- Call this function to serialize all tables on startup
  evalRepl (const $ pure ">>> ") cmd [] Nothing Nothing (Word completer) ini final

-- Your existing runExecuteIO function with addition for SaveFile
runExecuteIO :: Lib3.Execution r -> IO r
runExecuteIO (Pure r) = return r
runExecuteIO (Free step) = do
    next <- runStep step
    runExecuteIO next
    where
        runStep :: Lib3.ExecutionAlgebra a -> IO a
        runStep (Lib3.GetTime next) = getCurrentTime >>= return . next
        runStep (Lib3.SaveFile tableName content next) = do
            B.writeFile ("db/" ++ tableName ++ ".json") content
            return next
