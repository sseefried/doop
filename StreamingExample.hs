--
-- Sparse matrix multiplication in Hadoop
--
module StreamingExample where

-- standard library
import Data.List
import Text.Printf
-- friends
import HadoopStreaming


data MatrixId = MatA | MatB deriving Show
newtype Pos = Pos (Int, Int) deriving (Show, Eq, Ord)
type Val = Integer

type Key = (MatrixId, Pos)

--
-- | 10    20 |
-- |    30 40 |
-- | 50 60 70 |
--
matA :: [ (Pos, Val) ]
matA = [(Pos (1,1), 10), (Pos (1,3),20), (Pos (2,2),30), (Pos (2,3),40),
        (Pos (3,1),50), (Pos (3,2),60), (Pos (3,3), 70)]

--
-- | -1    |
-- | -2 -3 |
-- |    -4 |
matB :: [ (Pos, Val)]
matB = [ (Pos (1,1), -1), (Pos (2,1), -2), (Pos (2,2), -3), (Pos (3,2), -4) ]

matrixValues :: [(Key, Val)]
matrixValues = map (addId MatA) matA ++ map (addId MatB) matB
  where addId i (pos, val) = ((i,pos), val)

--
-- matA x matB =
-- | -10  -80  |
-- | -60  -250 |
-- | -170 -460 |
--
mapProd :: (Key, Val) -> [(Int, (Key, Val))]
mapProd kv@((matId, (Pos (row, col))), val) =
  case matId of
    MatA -> [(col, kv)]
    MatB -> [(row, kv)]

reduceProd :: (Int, [(Key, Val)]) -> [(Pos, Val)]
reduceProd (k, kvs) = [ a `mult` b | a <- as, b <- bs ]
  where
    (as,bs) = partition inA kvs
    inA ((MatA, _), _) = True
    inA _              = False
    mult :: (Key, Val) -> (Key, Val) -> (Pos, Val)
    mult ((MatA, Pos (row, col)), val) ((MatB, Pos (row', col')), val')
      | col == row' = (Pos (row, col'), val * val')
      | otherwise = error (printf "Column of matrix A not the same as row of matrix B %d /= %d"
                          (show col) (show col'))
    mult _ _ = error "Two values from the same matrix being multiplied"

mapSum :: (Pos, Val) -> [(Pos,Val)]
mapSum x = [x]

reduceSum :: (Pos, [Val]) -> [(Pos, Val)]
reduceSum (pos, vals) = [(pos, sum vals)]

matrixMult = run h
  where h = HadoopJob { mapper = mapProd, reducer = reduceProd } `HadoopComposeJob`
            HadoopJob { mapper = mapSum, reducer = reduceSum }
