package com.swoop.alchemy.utils

/**
  * Convenience methods for all types
  */
object AnyExtensions {

  /** Sugar for applying functions in a method chain. */
  implicit class TransformOps[A](val underlying: A) extends AnyVal {

    /** Applies a transformer function in a method chain.
      *
      * @param f function to apply
      * @tparam B the return type
      * @return the result of applying `f` on `underlying`.
      */
    @inline def transform[B](f: A => B): B =
      f(underlying)

    /** Conditionally applies a transformer function in a method chain.
      * Use this instead of [[transformWhen()]] when the predicate requires the value of `underlying`.
      *
      * @param predicate predicate to evaluate to determine if the function should be applied
      * @tparam B the return type of the function
      * @return `underlying` if the predicate evaluates to `false` or the result of function application.
      */
    @inline def transformIf[B <: A](predicate: A => Boolean)(f: A => B): A =
      if (predicate(underlying))
        f(underlying)
      else underlying

    /** Conditionally applies a transformer function in a method chain.
      * Use this instead of [[transformIf()]] when the condition does not require the value of `underlying`.
      *
      * @param condition condition to evaluate to determine if the function should be applied
      * @tparam B the return type of the function
      * @return `underlying` if the expression evaluates to `false` or the result of function application.
      */
    @inline def transformWhen[B <: A](condition: => Boolean)(f: A => B): A =
      if (condition)
        f(underlying)
      else underlying

  }

  /** Sugar for creating side-effects in method chains. */
  implicit class TapOps[A](val underlying: A) extends AnyVal {

    /** Applies a function for its side-effect as part of a method chain.
      * Inspired by Ruby's `Object#tap`.
      *
      * @param f side-effect function to call
      * @tparam B the return type of the function; ignored
      * @return `this`
      */
    @inline def tap[B](f: A => B): A = {
      f(underlying)
      underlying
    }

    /** Conditionally applies a function for its side-effect as part of a method chain.
      * Use this instead of [[tapWhen()]] when the predicate requires the value of `underlying`.
      *
      * @param predicate predicate to evaluate to determine if the side-effect should be invoked
      * @param f         side-effect function to call
      * @tparam B the return type of the function; ignored
      * @return `this`
      */
    @inline def tapIf[B](predicate: A => Boolean)(f: A => B): A = {
      if (predicate(underlying))
        f(underlying)
      underlying
    }

    /** Conditionally applies a function for its side-effect as part of a method chain.
      * Use this instead of [[tapIf()]] when the condition does not require the value of `underlying`.
      *
      * @param condition condition to evaluate to determine if the side-effect should be invoked
      * @param f         side-effect function to call
      * @tparam B the return type of the function; ignored
      * @return `this`
      */
    @inline def tapWhen[B](condition: => Boolean)(f: A => B): A = {
      if (condition)
        f(underlying)
      underlying
    }
  }

  /** Sugar for simple debugging/reporting by printing in a method chain. */
  implicit class PrintOps[A](val underlying: A) extends AnyVal {

    /** Taps and prints the object in a method chain.
      * Shorthand for `.tap(println)`.
      *
      * @return `underlying`
      */
    def tapp: A =
      underlying.tap(println)

    /** Prints a value as a side effect.
      *
      * @param v the value to print
      * @return `underlying`
      */
    def print[B](v: B): A =
      underlying.tap((_: A) => println(v))

    /** Conditionally taps and prints the object in a method chain.
      * Use this instead of [[printWhen()]] when the predicate requires the value of `underlying`.
      *
      * @param predicate predicate to evaluate to determine if the underlying value should be printed
      * @return `this`
      */
    def printIf(predicate: A => Boolean): A =
      underlying.tapIf(predicate)(println)

    /** Conditionally prints a value as a side effect.
      * Use this instead of [[printWhen()]] when the predicate requires the value of `underlying`.
      *
      * @param predicate predicate to evaluate to determine if the value should be printed
      * @param v         side-effect function to call
      * @tparam B the value type; ignored
      * @return `this`
      */
    def printIf[B](predicate: A => Boolean, v: B): A =
      underlying.tapIf(predicate)((_: A) => println(v))

    /** Conditionally taps and prints the object in a method chain.
      * Use this instead of [[printIf()]] when the condition does not require the value of `underlying`.
      *
      * @param condition condition to evaluate to determine if the underlying value should be printed
      * @return `underlying`
      */
    def printWhen(condition: => Boolean): A =
      underlying.tapWhen(condition)(println)

    /** Conditionally prints a value as a side effect.
      * Use this instead of [[printIf()]] when the condition does not require the value of `underlying`.
      *
      * @param condition condition to evaluate to determine if the value should be printed
      * @tparam B the value type; ignored
      * @return `underlying`
      */
    def printWhen[B](condition: => Boolean, v: B): A =
      underlying.tapWhen(condition)((_: A) => println(v))

  }

  /** Sugar for conditionally raising exceptions as part of a method chain. */
  implicit class ThrowOps[A](val underlying: A) extends AnyVal {

    /** Raises an exception if a predicate is satisfied.
      * Use this instead of [[throwWhen()]] when the predicate requires the value of `underlying`.
      *
      * @param predicate predicate to evaluate to determine if the exception should be thrown
      * @param e         expression that will return an exception
      * @tparam B the exception type
      * @return `underlying` if the predicate evaluates to `false`.
      * @throws B
      */
    def throwIf[B <: Throwable](predicate: A => Boolean)(e: => B): A = {
      if (predicate(underlying))
        throw e
      underlying
    }

    /** Raises an exception if a condition is satisfied.
      * Use this instead of [[throwIf()]] when the condition does not require the value of `underlying`.
      *
      * @param condition condition to evaluate to determine if the exception should be thrown
      * @param e         expression that will return an exception
      * @tparam B the exception type
      * @return `underlying` if the predicate evaluates to `false`.
      * @throws B
      */
    def throwWhen[B <: Throwable](condition: => Boolean, e: => B): A = {
      if (condition)
        throw e
      underlying
    }

  }

}
