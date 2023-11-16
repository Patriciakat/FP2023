module Main (main) where

import Control.Monad.IO.Class (MonadIO (liftIO))
import Control.Monad.Free (Free (..))
import Data.Functor ((<&>))
import Data.Time (UTCTime, getCurrentTime)
import Data.List qualified as L
import Data.Maybe (catMaybes)
import System.Console.Repline
import System.Console.Terminal.Size
import System.Directory (doesFileExist)
import qualified Data.ByteString.Lazy as B


import Lib1 qualified
import Lib2 qualified
import Lib3 qualified
import Lib3 (executeSql, Execution, ExecutionAlgebra(..), serializeDataFrame, deserializeDataFrame, readDataFrame, readDataFrameFile, saveDataFrame)
import DataFrame
import InMemoryTables (database, TableName)

type Repl a = HaskelineT IO a

-- Define the application state
data AppState = AppState
  { appDatabase :: [(TableName, DataFrame)]
  }

-- Final message when the REPL exits
final :: Repl ExitDecision
final = do
  liftIO $ putStrLn "Goodbye!"
  return Exit

-- Initialization message for the REPL
ini :: Repl ()
ini = liftIO $ putStrLn "Welcome to the select-manipulate database! Press [TAB] for auto completion."

-- Auto-completion for REPL commands
completer :: Monad m => WordCompleter m
completer n = return $ Prelude.filter (L.isPrefixOf n) commands
  where commands = ["select", "*", "from", "show", "tables", "insert", "into", "values", "set", "update", "delete"]

-- Command evaluation for the REPL
cmd :: AppState -> String -> Repl ()
cmd appState commandString = do
  s <- terminalWidth <$> liftIO size
  let parsedCommand = Lib2.parseStatement commandString
  case parsedCommand of
    Right statement -> do
      result <- liftIO $ executeCommand statement appState
      case result of
        Right df -> liftIO $ putStrLn $ Lib1.renderDataFrameAsTable s df
        Left errorMsg -> liftIO $ putStrLn $ "Error: " ++ errorMsg
    Left errorMsg -> liftIO $ putStrLn $ "Error parsing SQL: " ++ errorMsg
  where
    terminalWidth :: Integral n => Maybe (Window n) -> n
    terminalWidth = maybe 80 width

    executeCommand :: Lib2.MyParsedStatement -> AppState -> IO (Either String DataFrame)
    executeCommand statement state = do
      -- Here, you will need to handle different types of SQL commands
      -- For example, handling for Lib2.Insert would go here
      -- You will also need to update the AppState and persist the changes to JSON if needed
      -- For now, it returns a placeholder error message
      return $ Left "Command execution not yet implemented"

-- Initialize the state of the application from JSON files
initializeState :: IO (Maybe AppState)
initializeState = do
  let tableFiles = fmap (\(name, _) -> (name, "db/" ++ name ++ ".json")) database
  tables <- mapM loadTable tableFiles
  return . Just . AppState $ catMaybes tables
  where
    loadTable :: (TableName, FilePath) -> IO (Maybe (TableName, DataFrame))
    loadTable (name, file) = do
      fileExists <- doesFileExist file
      if fileExists
        then do
          maybeDf <- Lib3.readDataFrame file
          return $ fmap ((,) name) maybeDf
        else do
          let Just defaultTable = lookup name InMemoryTables.database
          B.writeFile file $ Lib3.serializeDataFrame defaultTable
          return $ Just (name, defaultTable)

-- The function to run the Execution monad
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
      runStep (Lib3.ReadFile filePath next) = do
          result <- Lib3.readDataFrame filePath
          return (next result)

-- Entry point of the application
main :: IO ()
main = do
  maybeState <- initializeState
  case maybeState of
    Just appState -> runApp appState
    Nothing       -> putStrLn "Failed to load initial state."
    
cmdWrapper :: AppState -> String -> Repl ()
cmdWrapper appState = cmd appState

-- Function to run the application with the given state
runApp :: AppState -> IO ()
runApp appState = do
  evalRepl (const $ pure ">>> ") (cmdWrapper appState) [] Nothing Nothing (Word completer) ini final