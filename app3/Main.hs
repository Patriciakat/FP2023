module Main (main) where

import Control.Monad.IO.Class (MonadIO (liftIO))
import Control.Monad.Free (Free (..))
import Data.Functor ((<&>))
import Data.Time (UTCTime, getCurrentTime)
import Data.List qualified as L
import Data.Char (toLower)
import Data.Maybe (catMaybes, fromJust, isJust, fromMaybe)
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

data AppState = AppState
  { appDatabase :: [(TableName, DataFrame)] }

-- Main function to start the REPL and handle initial state
main :: IO ()
main = do
  maybeState <- initializeState
  case maybeState of
    Just appState -> runApp appState
    Nothing       -> putStrLn "Failed to load initial state."

-- Function to run the application
runApp :: AppState -> IO ()
runApp appState = do
  evalRepl (const $ pure ">>> ") (cmd appState) [] Nothing Nothing (Word completer) ini final

-- Command function for the REPL
cmd :: AppState -> String -> Repl ()
cmd appState commandString = do
    unless (null commandString) $ do
        s <- terminalWidth <$> liftIO size
        let parsedCommand = Lib2.parseStatement commandString
        case parsedCommand of
            Right statement -> do
                (result, updatedState) <- liftIO $ executeCommand commandString appState
                case result of
                    Right df -> liftIO $ putStrLn $ Lib1.renderDataFrameAsTable s df
                    Left errorMsg -> liftIO $ putStrLn $ "Error: " ++ errorMsg
            Left errorMsg -> liftIO $ putStrLn $ "Error parsing SQL: " ++ errorMsg
    where
        terminalWidth :: Integral n => Maybe (Window n) -> n
        terminalWidth = maybe 80 width
        unless condition action = if not condition then action else return ()

-- Function to initialize the application state
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
          return $ Just (name, fromMaybe (getDefaultTable name) maybeDf)
        else do
          let defaultTable = getDefaultTable name
          B.writeFile file $ Lib3.serializeDataFrame defaultTable
          return . Just $ (name, defaultTable)

    getDefaultTable :: TableName -> DataFrame
    getDefaultTable name = 
      case lookup name InMemoryTables.database of
        Just df -> df
        Nothing -> error $ "Default table not found for " ++ name

-- Execution function for the Free monad
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
          
--------------------------------------------------HELPER FUNCTIONS------------------------------------------------------

-- Function to execute a command
executeCommand :: String -> AppState -> IO (Either String DataFrame, AppState)
executeCommand sql state = do
    result <- runExecuteIO $ Lib3.executeSql sql (appDatabase state)
    case result of
        Right df -> do
            updatedState <- refreshState state
            return (Right df, updatedState)
        Left errorMsg -> return (Left errorMsg, state)

-- Auto-completion function for REPL
completer :: Monad m => WordCompleter m
completer n = return $ Prelude.filter (L.isPrefixOf n) commands
  where commands = ["select", "*", "from", "show", "tables", "insert", "into", "values", "set", "update", "delete"]

-- Finalize REPL
final :: Repl ExitDecision
final = do
  liftIO $ putStrLn "Goodbye!"
  return Exit
  
-- Initialize REPL
ini :: Repl ()
ini = liftIO $ putStrLn "Welcome to the select-manipulate database! Press [TAB] for auto completion."

-- Helper functions to reload and refresh state
reloadTable :: TableName -> AppState -> IO AppState
reloadTable tableName (AppState db) = do
    maybeNewTable <- Lib3.readDataFrame $ "db/" ++ tableName ++ ".json"
    let updatedDb = case maybeNewTable of
                      Just newTable -> (tableName, newTable) : filter ((/= tableName) . fst) db
                      Nothing -> db
    return $ AppState updatedDb

refreshState :: AppState -> IO AppState
refreshState _ = do
    let tableFiles = fmap (\(name, _) -> (name, "db/" ++ name ++ ".json")) database
    tables <- mapM loadTable tableFiles
    return $ AppState $ catMaybes tables
  where
    loadTable :: (TableName, FilePath) -> IO (Maybe (TableName, DataFrame))
    loadTable (name, file) = do
        maybeDf <- Lib3.readDataFrame file
        return $ Just (name, fromMaybe (getDefaultTable name) maybeDf)
 
getDefaultTable :: TableName -> DataFrame
getDefaultTable name = 
    case lookup name InMemoryTables.database of
        Just df -> df
        Nothing -> error $ "Default table not found for " ++ name