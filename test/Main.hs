{-# LANGUAGE
    OverloadedStrings
  #-}

module Main where

import           Control.Applicative
import           Control.Monad.Trans
import           Data.ByteString(ByteString)
import qualified Data.ByteString.Lazy.Char8 as BSL

import           Vaultaire.Collector.Common.Process

import           Ceilometer.Process
import           Ceilometer.Types


runTestPublisher :: Publisher () -> IO ()
runTestPublisher = runCollector (pure $ CeilometerOptions "" "" "" True True "" 0) (\_ -> return $ CeilometerState undefined undefined) (return ())

main = runTestPublisher $ do
    s <- liftIO $ BSL.readFile "../../../test/volume.json"
    processedVolume <- processSample s
    liftIO $ case processedVolume of
        [] -> putStrLn "volume failed to process"
        [(addr, sd, ts, p)] -> putStrLn "succeeded processing volume"
        xs -> putStrLn "too many things"
