{-# LANGUAGE
    OverloadedStrings
  #-}

module Main where

import           Control.Applicative
import           Control.Monad.Trans
import           Data.Bits
import           Data.ByteString(ByteString)
import qualified Data.ByteString.Lazy.Char8 as BSL
import           Data.HashMap.Strict(HashMap)
import qualified Data.HashMap.Strict as H
import           Data.Text(Text)
import           Data.Word
import           Test.Hspec
import           Test.HUnit.Base

import           Vaultaire.Types
import           Vaultaire.Collector.Common.Process

import           Ceilometer.Process
import           Ceilometer.Types


runTestPublisher :: Publisher a -> IO a
runTestPublisher = runCollector (pure $ CeilometerOptions "" "" "" True True "" 0) (\_ -> return $ CeilometerState undefined undefined) (return ())

expectedVolumePayload :: Word64
expectedVolumePayload = 2 + (1 `shift` 8) + (1 `shift` 16) + (10 `shift` 32)

expectedVolumeTimestamp :: TimeStamp
expectedVolumeTimestamp = TimeStamp 1411371101378773000

expectedVolumeHashmap :: HashMap Text Text
expectedVolumeHashmap = H.fromList
  [ ("_event", "1"),
    ("_compound", "1"),
    ("project_id", "aaaf752c50804cf3aad71b92e6ced65e"),
    ("resource_id", "6b116a55-2716-4406-9304-0080e3a5c608"),
    ("metric_name", "volume.size"),
    ("metric_unit", "GB"),
    ("metric_type", "gauge"),
    ("display_name", "Worpress0Snapshot3")
  ]

expectedVolumeSd :: SourceDict
expectedVolumeSd = either error id (makeSourceDict expectedVolumeHashmap)

expectedIPFloatingPayload :: Word64
expectedIPFloatingPayload = 2 + (1 `shift` 8) + (2 `shift` 16) + (1 `shift` 32)

expectedIPFloatingTimestamp :: TimeStamp
expectedIPFloatingTimestamp = TimeStamp 1411371303030569000

expectedIPFloatingHashmap :: HashMap Text Text
expectedIPFloatingHashmap = H.fromList
  [ ("_event", "1"),
    ("_compound", "1"),
    ("project_id", "aaaf752c50804cf3aad71b92e6ced65e"),
    ("resource_id", "d1d96a82-c1ce-4feb-acc1-227a50be9b9f"),
    ("metric_name", "ip.floating"),
    ("metric_unit", "ip"),
    ("metric_type", "gauge"),
    ("display_name", "")
  ]

expectedIPFloatingSd :: SourceDict
expectedIPFloatingSd = either error id (makeSourceDict expectedIPFloatingHashmap)

suite :: Spec
suite = do
    describe "Processing Supported Metrics" $ do
        it "Processes volume.size events" testVolume
        it "Processes ip.floating events" testIPFloating
        it "Processes instance pollsters" pending
        it "Processes network tx/rx pollsters" pending
        it "Processes disk read/write pollsters" pending
        it "Processes cpu usage pollsters" pending
    describe "Ignoring Unsupported Metrics" $ do
        it "Ignores disk read/write requests pollsters" pending
        it "Ignores specifically sized instance pollsters" pending
    describe "Utility" $
        it "Processes timestamps correctly" pending

main :: IO ()
main = hspec suite

testVolume :: IO ()
testVolume = runTestPublisher $ do
    rawJSON <- liftIO $ BSL.readFile "test/json_files/volume.json"
    processedVolume <- processSample rawJSON
    liftIO $ case processedVolume of
        [] -> assertFailure "processedVolume failed, expected 1 element"
        [x@(_, sd, ts, p)] -> do
            sd @?= expectedVolumeSd
            ts @?= expectedVolumeTimestamp
            p  @?= expectedVolumePayload
        xs -> do
            let n = xs
            assertFailure $ concat ["processedVolume has ", show n, " elements:, ", show xs, ". Expected 1"]

testIPFloating :: IO ()
testIPFloating = runTestPublisher $ do
    rawJSON <- liftIO $ BSL.readFile "test/json_files/ip_floating.json"
    processedIPFloating <- processSample rawJSON
    liftIO $ case processedIPFloating of
        [] -> assertFailure "processedIPFloating failed, expected 1 element"
        [x@(_, sd, ts, p)] -> do
            sd @?= expectedIPFloatingSd
            ts @?= expectedIPFloatingTimestamp
            p  @?= expectedIPFloatingPayload
        xs -> do
            let n = xs
            assertFailure $ concat ["processedIPFloating has ", show n, " elements:, ", show xs, ". Expected 1"]
