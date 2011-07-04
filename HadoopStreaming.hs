{-# LANGUAGE ExistentialQuantification #-}
--
-- In this module I'm prototyping the streaming version of Doop.
--
module HadoopStreaming where

import Data.List

--
-- | A type representing a Hadoop jobs that takes
--   key/value pairs of type (ki,vi) as input
--   and returns key/value pairs of type (ko,vo)
--
data Hadoop ki vi ko vo =
    forall k v. Ord k => HadoopJob { mapper  :: (ki,vi)    -> [(k, v)]
                                   , reducer :: (k, [v])   -> [(ko, vo)] }
  -- | Allows you to compose two Hadoop jobs together.
  | forall k v. HadoopComposeJob (Hadoop ki vi k v) (Hadoop k v ko vo)
--
-- Takes a Hadoop job and runs it.
--
run :: Hadoop ki vi ko vo -> [(ki,vi)] -> [(ko, vo)]
run (HadoopJob mapper reducer) input = reduced
  where
    mapped  = concatMap mapper input
    sorted  = map (\xs -> let (k,_) = head xs in (k, map snd xs))
            $ groupBy (byKey (==))
            $ sortBy (byKey compare) mapped

    reduced = concatMap reducer sorted

    byKey :: (a -> a -> b) -> (a, c) -> (a, c) -> b
    byKey f (k,_) (k', _) = f k k'
run (HadoopComposeJob h1 h2) input = (run h2) . (run h1) $ input