module Main (main) where

import Control.Monad.IO.Class (MonadIO (liftIO))
import Data.List qualified as L
import InMemoryTables (database)
import Lib1 qualified
import Lib2 qualified
import System.Console.Repline
  ( CompleterStyle (Word),
    ExitDecision (Exit),
    HaskelineT,
    WordCompleter,
    evalRepl,
  )
import System.Console.Terminal.Size (Window, size, width)

type Repl a = HaskelineT IO a

final :: Repl ExitDecision
final = do
  liftIO $ putStrLn "Goodbye!"
  return Exit

ini :: Repl ()
ini = liftIO $ putStrLn "Welcome to select-more database! Press [TAB] for auto completion."

completer :: (Monad m) => WordCompleter m
completer n = do
  let names = ["select", "*", "from", "show", "table", "tables"] ++ map fst database
  return $ Prelude.filter (L.isPrefixOf n) names

-- Evaluation: handle each line user inputs
cmd :: String -> Repl ()
cmd c = do
  s <- terminalWidth <$> liftIO size
  result <- liftIO $ do -- wrap the IO actions with liftIO
    case Lib2.parseStatement c of
      Right stmt -> Lib2.executeStatement stmt
      Left err -> return $ Left err
  case result of
    Left err -> liftIO $ putStrLn $ "Error: " ++ err
    Right df -> do
      let validation = Lib1.validateDataFrame df
      case validation of
        Left err -> liftIO $ putStrLn err
        Right _ -> liftIO $ putStrLn $ Lib1.renderDataFrameAsTable s df
  where
    terminalWidth :: (Integral n) => Maybe (Window n) -> n
    terminalWidth = maybe 80 width

main :: IO ()
main =
  evalRepl (const $ pure ">>> ") cmd [] Nothing Nothing (Word completer) ini final