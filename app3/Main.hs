module Main (main) where

import Control.Monad (unless)
import Control.Monad.IO.Class (MonadIO (liftIO))
import Control.Monad.Free (Free (..), liftF)
import Data.Time (UTCTime, getCurrentTime)
import Data.List qualified as L
import Data.Maybe (catMaybes, fromJust, isJust, fromMaybe)
import Data.Time.Format (formatTime, defaultTimeLocale)
import System.Console.Repline
import System.Console.Terminal.Size
import System.Directory (doesFileExist)
import qualified Data.ByteString.Lazy as B
import Lib1 qualified
import Lib2 qualified
import Lib3 qualified
import Lib3 (executeParsedStatement, handleNowStatement, Execution, ExecutionAlgebra(..), serializeDataFrame, deserializeDataFrame, readDataFrame, readDataFrameFile, saveDataFrame)
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
                cmd updatedState ""
            Left errorMsg -> liftIO $ putStrLn $ "Error parsing SQL: " ++ errorMsg
    where
        terminalWidth :: Integral n => Maybe (Window n) -> n
        terminalWidth = maybe 80 width

initializeState :: IO (Maybe AppState)
initializeState = do
  let tableFiles = fmap (\(name, _) -> ("db/" ++ name ++ ".json", name)) database
  tables <- mapM (\(file, name) -> loadTable name file) tableFiles
  return $ Just $ AppState $ catMaybes tables

-- Execution function for the Free monad
runExecuteIO :: Bool -> Lib3.Execution r -> IO r
runExecuteIO isTest (Pure r) = return r
runExecuteIO isTest (Free step) = do
    next <- runStep isTest step
    runExecuteIO isTest next
    where
      runStep :: Bool -> Lib3.ExecutionAlgebra a -> IO a
      runStep isTest (Lib3.LoadFile filePath next) = do
          fileContent <- B.readFile filePath
          return (next fileContent)
      runStep isTest (Lib3.SaveFile tableName content next) = do
          B.writeFile ("db/" ++ tableName ++ ".json") content
          return next
      runStep isTest (Lib3.ExecuteSqlStatement statement next) = do
          if statement == Lib2.Now
          then Lib3.handleNowStatement next
          else if isTest
               then let result = Lib3.executeParsedStatement statement Lib3.initialInMemoryDB
                    in return $ next result
               else do
                    result <- Lib2.executeStatement statement
                    return $ next result
      runStep isTest (Lib3.GetTime next) = do
          currentTime <- getCurrentTime
          return $ next currentTime
        
          
--------------------------------------------------HELPER FUNCTIONS------------------------------------------------------


-- Function to execute a command
executeCommand :: String -> AppState -> IO (Either String DataFrame, AppState)
executeCommand sql state = do
    case Lib2.parseStatement sql of
        Right parsedStatement -> do
            result <- runExecuteIO False (liftF $ Lib3.ExecuteSqlStatement parsedStatement id)
            case result of
                Right df -> do
                    updatedState <- refreshState state
                    return (Right df, updatedState)
                Left errorMsg -> return (Left errorMsg, state)
        Left parseError -> return (Left ("Error parsing SQL: " ++ parseError), state)

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

refreshState :: AppState -> IO AppState
refreshState _ = do
    let tableFiles = fmap (\(name, _) -> ("db/" ++ name ++ ".json", name)) database
    tables <- mapM (\(file, name) -> loadTable name file) tableFiles
    return $ AppState $ catMaybes tables
    
loadTable :: TableName -> FilePath -> IO (Maybe (TableName, DataFrame))
loadTable name file = do
  fileExists <- doesFileExist file
  if fileExists
    then do
      maybeDf <- Lib3.readDataFrame file
      return $ Just (name, fromMaybe (getDefaultTable name) maybeDf)
    else do
      let defaultTable = getDefaultTable name
      B.writeFile file $ Lib3.serializeDataFrame defaultTable
      return $ Just (name, defaultTable)
 
getDefaultTable :: TableName -> DataFrame
getDefaultTable name =
  case lookup name InMemoryTables.database of
    Just df -> df
    Nothing -> error $ "Default table not found for " ++ name