--
-- Sparse matrix multiplication in Hadoop
--
module Example where

-- standard library
import Data.List
import Text.Printf
-- friends
import Hadoop

h = HadoopJob { mapper  = Lam (\x -> List [x :: Exp (Int,Int)])
              , reducer = Lam (\x -> List [ Tuple (Fst x, foldlExp (+) (0::Exp Int) (Snd x) )] ) }

foldlExp :: Lift b => (Exp a -> Exp b -> Exp a) -> Exp a -> Exp [b] -> Exp a
foldlExp f z xs = foldl f z (map lift (eval xs))

-- run h test == [(1,5),(2,5),(3,13)]
test = run h ([(1,2), (1,3), (3,7), (2,5), (3,6)])


{--
type MatrixId = Int
type Pos = (Int, Int)
type Val = Int

type Key = (MatrixId, Pos)

--
-- | 10    20 |
-- |    30 40 |
-- | 50 60 70 |
--
matA :: [ (Pos, Val) ]
matA = [((1,1), 10), ((1,3),20), ((2,2),30), ((2,3),40), ((3,1),50), ((3,2),60), ((3,3), 70)]


--
-- | -1    |
-- | -2 -3 |
-- |    -4 |
matB :: [ (Pos, Val)]
matB = [ ((1,1), -1), ((2,1), -2), ((2,2), -3), ((3,2), -4) ]


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
--}