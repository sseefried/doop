{-# LANGUAGE GADTs, ExistentialQuantification, FlexibleInstances, StandaloneDeriving #-}
{-# LANGUAGE ScopedTypeVariables, UndecidableInstances #-}
--
-- In this module we are prototyping the Java-backend approach
--
module Hadoop where

import Text.Printf
import Data.List

data Hadoop ki vi ko vo =
    forall k v. Ord k => HadoopJob { mapper  :: Exp ((ki,vi)  -> [(k, v)])
                                   , reducer :: Exp ((k, [v]) -> [(ko, vo)]) }
  -- | Allows you to compose two Hadoop jobs together.
  | forall k v. HadoopComposeJob (Hadoop ki vi k v) (Hadoop k v ko vo)

-- Anything that can be an element
class (Show a, Eq a) => Elt a

instance Elt Int

class (Elt a, Num a) => NumElt a

instance NumElt Int

data Exp a where
  App   :: Show a => Exp (a -> b) -> Exp a -> Exp b
  Lam       :: (Exp a -> Exp b) -> Exp (a -> b)
  Add       :: NumElt a => Exp (a, a) -> Exp a
  Mult      :: NumElt a => Exp (a, a) -> Exp a
  Const     :: a -> Exp a
  List      :: Elt a => [Exp a] -> Exp [a]
  Head      :: Exp [a] -> Exp a
  Tail      :: Exp [a] -> Exp [a]
  Tuple     :: (Elt a, Elt b) => (Exp a, Exp b) -> Exp (a,b)
  Fst       :: Elt b => Exp (a,b) -> Exp a
  Snd       :: Elt a => Exp (a,b) -> Exp b

instance Eq (Exp Int) where
  (==) = error "Eq is not defined on Exp type"

instance Show a => Show (Exp a) where
  show exp = case exp of
    App fun arg   -> printf "PrimApp <function> (%s)" (show arg)
    Lam body      -> "Lam <body>"
    Add _         -> printf "Add"
    Mult _        -> printf "Mult"
    Const a       -> printf "Const %s" (show a)
    List  xs      -> printf "List %s" (show xs)
    Head  xs      -> printf "Head (%s)" (show xs)
    Tail  xs      -> printf "Tail (%s)" (show xs)
    Tuple tup     -> printf "Tuple %s" (show tup)
    Fst   tup     -> printf "Fst (%s)" (show tup)
    Snd   tup     -> printf "Snd (%s)" (show tup)

instance Num (Exp Int) where
  x + y       = App (Lam Add) $ Tuple (x,y)
  fromInteger = Const . fromInteger
  x * y       = App (Lam Mult) $ Tuple (x,y)
  abs         = error "not defined"
  signum      = error "not defined"

---

class Lift a where
  lift :: a -> Exp a

instance (Elt a, Elt b, Lift a, Lift b) => Lift (a,b) where
  lift (a,b) = Tuple (lift a, lift b)

instance Lift Int where
  lift = Const

instance (Elt a, Lift a) => Lift [a] where
  lift xs = List (map lift xs)

hd :: Exp [a] -> Exp a
hd = Head

tl :: Exp [a] -> Exp [a]
tl = Tail


--
-- Interpreter
--

eval :: Exp a -> a
eval (App (Lam fun) arg) = eval (fun arg)
eval (Add pair)   = let (x,y) = eval pair in x + y
eval (Mult pair)  = let (x,y) = eval pair in x * y
eval (Const a)     = a
eval (List xs)     = map eval xs
eval (Head xs)     = head (eval xs)
eval (Tail xs)     = tail (eval xs)
eval (Tuple (a,b)) = (eval a, eval b)
eval (Fst tup)     = fst (eval tup)
eval (Snd tup)     = snd (eval tup)

evalMap :: Exp (a -> b) -> [a] -> [b]
evalMap fun xs = map (eval fun) xs

evalConcatMap :: Exp (a -> [b]) -> [a] -> [b]
evalConcatMap f = concat . evalMap f

run :: Hadoop ki vi ko vo -> [(ki,vi)] -> [(ko,vo)]
run (HadoopJob mapper reducer) input = reduced
  where
    mapped  = evalConcatMap mapper input
    sorted  = map (\xs -> let (k,_) = head xs in (k, map snd xs))
            $ groupBy (byKey (==))
            $ sortBy (byKey compare) mapped

    reduced = evalConcatMap reducer sorted

    byKey :: (a -> a -> b) -> (a, c) -> (a, c) -> b
    byKey f (k,_) (k', _) = f k k'
run (HadoopComposeJob h1 h2) input = (run h2) . (run h1) $ input