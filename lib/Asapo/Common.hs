module Asapo.Common (SourceType (..), InstanceId (..), PipelineStep (..), Beamline (..), Beamtime (..), DataSource (..), Token (..), SourceCredentials (..)) where

import Data.Text (Text)
import Prelude ()

data SourceType = RawSource | ProcessedSource

newtype InstanceId = InstanceId Text

newtype PipelineStep = PipelineStep Text

newtype Beamtime = Beamtime Text

newtype Beamline = Beamline Text

newtype DataSource = DataSource Text

newtype Token = Token Text

data SourceCredentials = SourceCredentials
  { sourceType :: SourceType,
    instanceId :: InstanceId,
    pipelineStep :: PipelineStep,
    beamtime :: Beamtime,
    beamline :: Beamline,
    dataSource :: DataSource,
    token :: Token
  }
