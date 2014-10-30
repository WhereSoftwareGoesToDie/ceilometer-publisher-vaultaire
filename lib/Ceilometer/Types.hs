{-# LANGUAGE OverloadedStrings #-}

module Ceilometer.Types where

import           Control.Applicative
import           Data.Aeson
import           Data.Aeson.Types
import           Data.HashMap.Strict(HashMap)
import           Data.Text(Text)
import           Data.Word
import           Network.AMQP

import           Vaultaire.Collector.Common.Types
import           Vaultaire.Types

data Metric = Metric
    { metricName        :: Text
    , metricType        :: Text
    , metricUOM         :: Text
    , metricPayload     :: Word64
    , metricProjectId   :: Text
    , metricResourceId  :: Text
    , metricTimeStamp   :: TimeStamp
    , metricMetadata    :: HashMap Text Value
    } deriving Show

data Flavor = Flavor
    { instanceVcpus     :: Word64
    , instanceRam       :: Word64
    , instanceDisk      :: Word64
    , instanceEphemeral :: Word64
    } deriving Show

data CeilometerOptions = CeilometerOptions
    { rabbitLogin :: Text
    , rabbitVHost :: Text
    , rabbitHost  :: String
    , rabbitHa    :: Bool
    , rabbitUseSSL :: Bool
    , rabbitExchange   :: Text
    , rabbitPollPeriod :: Int
    }

data CeilometerState = CeilometerState
    { ceilometerMessageConn :: Connection
    , ceilometerMessageChan :: Channel
    }

type PublisherProducer = CollectionStream CeilometerOptions CeilometerState IO

instance FromJSON Metric where
    parseJSON (Object s) = Metric
        <$> s .: "name"
        <*> s .: "type"
        <*> s .: "unit"
        <*> s .: "volume"
        <*> s .: "project_id"
        <*> s .: "resource_id"
        <*> (convertToTimeStamp <$> s .: "timestamp")
        <*> s .: "resource_metadata"
    parseJSON o = error $ "Cannot parse metrics from non-objects. Given: " ++ show o

instance FromJSON Flavor where
    parseJSON (Object s) = Flavor
        <$> s .: "vcpus"
        <*> s .: "ram"
        <*> s .: "disk"
        <*> s .: "ephemeral"
    parseJSON o = error $ "Cannot parse flavor from non-objects. Given: " ++ show o

metricParserWithName :: Text -> Value -> Parser Metric
metricParserWithName name o = do
    b <- parseJSON o
    if metricName b == name then
        return b
    else
        error $ concat ["Cannot decode: ", show o, " as ", show name, " metric."]
