{-# LANGUAGE
    OverloadedStrings
  , RecordWildCards
  #-}

module Ceilometer.Process where

import           Control.Applicative
import           Control.Concurrent hiding (yield)
import           Control.Monad
import           Control.Monad.Reader
import           Control.Monad.State
import           Crypto.MAC.SipHash(SipHash(..), SipKey(..), hash)
import           Data.Aeson
import           Data.Bits
import qualified Data.ByteString as S
import qualified Data.ByteString.Lazy.Char8 as L
import           Data.HashMap.Strict(HashMap)
import qualified Data.HashMap.Strict as H
import           Data.Maybe
import           Data.Monoid
import           Data.Word
import           Data.Text(Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import           Network.AMQP
import           Options.Applicative hiding (Success)
import           Pipes
import           System.Log.Logger

import           Marquise.Client
import           Vaultaire.Collector.Common.Process

import           Ceilometer.Types

runPublisher :: IO ()
runPublisher = do
    opts <- execParser (info parseOptions fullDesc)
    runCollector opts initState cleanup processSamples
  where
    parseOptions = CeilometerOptions
        <$> strOption
            (long "host-name"
             <> short 'h'
             <> metavar "HOST-NAME"
             <> help "URI of message queue to use")
        <*> option auto
            (long "poll-period"
             <> short 'p'
             <> value 1000000
             <> metavar "POLL-PERIOD"
             <> help "Time to wait (in microseconds) before re-querying empty queue.")
        <*> option auto
            (long "exchange"
             <> short 'e'
             <> value "ceilometer"
             <> metavar "CONTROL-EXCHANGE"
             <> help "Name of the ceilometer control exchange.")
    initState (_, CeilometerOptions{..}) = do
        conn <- openConnection ceilometerMessageURI "/" "" ""
        chan <- openChannel conn
        (q, _, _) <- declareQueue chan newQueue
        bindQueue chan q ceilometerExchange ""
        return $ CeilometerState conn chan

    cleanup = do
        (_, CeilometerState conn _) <- get
        liftIO $ closeConnection conn

processSamples :: PublisherProducer
processSamples = forever $ do
    (_, CeilometerOptions{..}) <- lift ask
    (_, CeilometerState{..}) <- lift get
    msg <- liftIO $ getMsg ceilometerMessageChan Ack "queue_name"
    case msg of
        Nothing          -> liftIO $ threadDelay ceilometerPollPeriod
        Just (msg', env) -> do
            processSample $ msgBody msg'
            liftIO $ ackEnv env

processSample :: L.ByteString -> PublisherProducer
processSample bs =
    case decode bs of
        Nothing           -> liftIO $ alertM "Ceilometer.Process.processSample" $ "Failed to parse: " ++ show bs
        Just m@Metric{..} -> case (metricName, isEvent m) of
            ("instance", False) -> publishInstance m
            ("cpu", False) -> publishBasePollster m
            ("disk.write.bytes", False) -> publishBasePollster m
            ("disk.read.bytes", False) -> publishBasePollster m
            ("network.incoming.bytes", False) -> publishBasePollster m
            ("network.outgoing.bytes", False) -> publishBasePollster m
            ("ip.floating", True) -> publishIpEvent m
            ("volume.size", True) -> publishVolumeEvent m
            (x, y) -> liftIO $ infoM "Ceilometer.Process.processSample" $ concat ["Unsupported metric: ", show x, " event: ", show y]

isEvent :: Metric -> Bool
isEvent m = H.member "event_type" $ metricMetadata m

getEventType :: Metric -> Maybe Text
getEventType m = case H.lookup "event_type" $ metricMetadata m of
    Just (String x) -> Just x
    _               -> Nothing

isCompound :: Metric -> Bool
isCompound m
    | isEvent m && metricName m == "ip.floating" = True
    | isEvent m && metricName m == "volume.size" = True
    | otherwise                                  = False

getSourceMap :: Metric -> HashMap Text Text
getSourceMap m@Metric{..} =
    let base = [ ("_event", if isEvent m then "1" else "0")
               , ("_compound", if isCompound m then "1" else "0")
               , ("project_id",   metricProjectId)
               , ("resource_id",  metricResourceId)
               , ("metric_name",  metricName)
               , ("metric_unit",  metricUOM)
               , ("metric_type",  metricType)
               , ("display_name", displayName)
               ]
        displayName = case H.lookup "display_name" metricMetadata of
            Just (String x) -> x
            _               -> ""
        counter = [("_counter", "1") | metricType == "cumulative"]
    in H.fromList $ counter ++ base

mapToSourceDict :: HashMap Text Text -> IO (Maybe SourceDict)
mapToSourceDict sourceMap = case makeSourceDict sourceMap of
    Left err -> do
        alertM "Ceilometer.Process.getSourceDict" $
            concat ["Failed to create sourcedict from ", show sourceMap, " error: ", err]
        return Nothing
    Right sd -> return $ Just sd

getIdElements :: Metric -> Text -> [Text]
getIdElements m@Metric{..} name =
    let base     = [metricProjectId, metricResourceId, metricUOM, metricType, name]
        event    = if isEvent m then
                       ["_event", fromJust $ getEventType m]
                   else []
        compound = ["_compound" | isCompound m]
    in concat [base,event,compound]

getAddress :: Metric -> Text -> Address
getAddress m name = hashIdentifier $ T.encodeUtf8 $ mconcat $ getIdElements m name

publishBasePollster :: Metric -> PublisherProducer
publishBasePollster m@Metric{..} = do
    sd <- liftIO $ mapToSourceDict $ getSourceMap m
    case sd of
        Just sd' -> do
            let addr = getAddress m metricName
            yield (addr, Left sd')
            yield (addr, Right (SimplePoint addr metricTimeStamp metricPayload))
        Nothing -> return ()

publishInstance :: Metric -> PublisherProducer
publishInstance m@Metric{..} = do
    let baseMap = getSourceMap m
    let names = ["instance_vcpus", "instance_ram", "instance_disk", "instance_flavor"]
    let uoms  = ["vcpu"          , "MB"          , "GB"           , "instance"       ]
    let addrs = map (getAddress m) names
    let sourceMaps = map (\(name, uom) -> H.insert "metric_unit" uom $ H.insert "metric_name" name baseMap)
            (zip names uoms)
    sds <- liftIO $ catMaybes <$> forM sourceMaps mapToSourceDict

    if length sds == 4 then
        case fromJSON $ fromJust $ H.lookup "flavor" metricMetadata of
            Error e -> liftIO $ alertM "Ceilometer.Process.publishInstance" $
                "Failed to parse flavor sub-object for instance pollster" ++ show e
            Success Flavor{..} -> do
                let (String instanceType) = fromJust $ H.lookup "instance_type" metricMetadata
                let instanceType' = siphash $ T.encodeUtf8 instanceType
                let payloads = [instanceVcpus, instanceRam, instanceDisk + instanceEphemeral, instanceType']
                forM_ (zip3 addrs sds payloads) $ \(addr, sd, p) -> do
                    yield (addr, Left sd)
                    yield (addr, Right (SimplePoint addr metricTimeStamp p))
    else
        liftIO $ alertM "Ceilometer.Process.publishInstance"
            "Failure to convert all sourceMaps to SourceDicts for instance pollster"

publishVolumeEvent :: Metric -> PublisherProducer
publishVolumeEvent = publishEvent getVolumePayload

publishIpEvent :: Metric -> PublisherProducer
publishIpEvent = publishEvent getIpPayload

publishEvent :: (Metric -> IO (Maybe Word64)) -> Metric -> PublisherProducer
publishEvent f m@Metric{..} = do
    p <- liftIO $ f m
    case p of
        Nothing -> return ()
        Just compoundPayload -> do
            sd <- liftIO $ mapToSourceDict $ getSourceMap m
            case sd of
                Just sd' -> do
                    let addr = getAddress m metricName
                    yield (addr, Left sd')
                    yield (addr, Right (SimplePoint addr metricTimeStamp compoundPayload))
                Nothing -> return ()

getVolumePayload :: Metric -> IO (Maybe Word64)
getVolumePayload m@Metric{..} = do
    let [_, verb, endpoint, _] = T.splitOn "." $ fromJust $ getEventType m
    let (String status)  = fromJust $ H.lookup "status" metricMetadata
    statusValue <- case status of
        "available" -> return 1
        "creating"  -> return 2
        "extending" -> return 3
        "deleting"  -> return 4
        x           -> do
            alertM "Ceilometer.Process.getVolumePayload" $
                "Invalid status for volume event: " ++ show x
            return 0
    verbValue <- case verb of
        "create" -> return 1
        "resize" -> return 2
        "delete" -> return 3
        x        -> do
            alertM "Ceilometer.Process.getVolumePayload" $
                "Invalid verb for volume event: " ++ show x
            return 0
    endpointValue <- case endpoint of
        "start" -> return 1
        "end"   -> return 2
        x       -> do
            alertM "Ceilometer.Process.getVolumePayload" $
                "Invalid endpoint for volume event: " ++ show x
            return 0
    return $ if 0 `elem` [statusValue, verbValue, endpointValue] then
        Nothing
    else
        Just $ constructCompoundPayload statusValue verbValue endpointValue metricPayload

getIpPayload :: Metric -> IO (Maybe Word64)
getIpPayload m@Metric{..} = do
    let [_, verb, endpoint, _] = T.splitOn "." $ fromJust $ getEventType m
    let (String status)  = fromJust $ H.lookup "status" metricMetadata
    statusValue <- case status of
        "ACTIVE" -> return 1
        "DOWN"   -> return 2
        x        -> do
            alertM "Ceilometer.Process.getIpPayload" $
                "Invalid status for ip event: " ++ show x
            return 0
    verbValue <- case verb of
        "create" -> return 1
        "update" -> return 2
        x        -> do
            alertM "Ceilometer.Process.getIpPayload" $
                "Invalid verb for ip event: " ++ show x
            return 0
    endpointValue <- case endpoint of
        "start" -> return 1
        "end"   -> return 2
        x       -> do
            alertM "Ceilometer.Process.getIpPayload" $
                "Invalid endpoint for ip event: " ++ show x
            return 0
    return $ if 0 `elem` [statusValue, verbValue, endpointValue] then
        Nothing
    else
        Just $ constructCompoundPayload statusValue verbValue endpointValue 0

constructCompoundPayload :: Word64 -> Word64 -> Word64 -> Word64 -> Word64
constructCompoundPayload statusValue verbValue endpointValue rawPayload =
    let s = statusValue
        v = verbValue `shift` 8
        e = endpointValue `shift` 16
        r = 0 `shift` 24
        p = rawPayload `shift` 32
    in
        s + v + e + r + p

siphash :: S.ByteString -> Word64
siphash x = let (SipHash h) = hash (SipKey 0 0) x in h
