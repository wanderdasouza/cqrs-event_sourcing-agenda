package com.twitter.util

object Local {

  /**
   * A key value structure with better run-time performance characteristics
   * when the number of elements is less than 16.
   * key - Local.Key, value - Option[_]
   */
  sealed abstract class Context private () {
    private[util] def get(k: Key): Option[_]
    private[util] def remove(k: Key): Context
    private[util] def set(k: Key, v: Some[_]): Context
  }

  object Context {

    /**
     * The empty Context
     */
    def empty: Context = EmptyContext

    private object EmptyContext extends Context {
      def get(k: Key): Option[_] = None
      def remove(k: Key): Context = this
      def set(k: Key, v: Some[_]): Context = new Context1(k, v)
    }

    private final class Context1(
      k1: Key,
      v1: Some[_]
    ) extends Context {

      def get(k: Key): Option[_] =
        if (k eq k1) v1 else None

      def remove(k: Key): Context =
        if (k eq k1) EmptyContext else this

      def set(k: Key, v: Some[_]): Context =
        if (k eq k1) new Context1(k1, v)
        else new Context2(k1, v1, k, v)
    }

    private final class Context2(
      k1: Key,
      v1: Some[_],
      k2: Key,
      v2: Some[_]
    ) extends Context {

      def get(k: Key): Option[_] =
        if (k eq k1) v1
        else if (k eq k2) v2
        else None

      def remove(k: Key): Context =
        if (k eq k1) new Context1(k2, v2)
        else if (k eq k2) new Context1(k1, v1)
        else this

      def set(k: Key, v: Some[_]): Context =
        if (k eq k1) new Context2(k1, v, k2, v2)
        else if (k eq k2) new Context2(k1, v1, k2, v)
        else new Context3(k1, v1, k2, v2, k, v)
    }

    // Script for generating the ContextN
    /*
    object ContextGen {

      def mkContext(num: Int): String = {
        s"""private final class Context$num(
    ${fieldList(num)}
    ) extends Context {

      ${mkGet(num)}

      ${mkRemove(num)}

      ${mkSet(num)}
    }

    """
      }

      def fieldList(num: Int) : String = {
        (1.to(num)).map{ i =>
          s"\tk$i: Key, v$i: Some[_]"
        }.mkString(", \n")
      }

      def mkGet(num: Int): String = {
        val name = s"def get(k: Key): Option[_] = "
        val content = (1.to(num)).map { i =>
          s"k$i) v$i"
        }.mkString("\n\t\tif (k eq ", "\n\t\telse if (k eq ", "\n\t\telse None")
        name + content
      }

      def mkRemove(num: Int): String = {
        val name = s"def remove(k: Key): Context = "
        def newCtx(sum: Int, current: Int): String = {
          (1.to(current - 1) ++: (current + 1).to(sum)).map { i =>
            s"k$i, v$i"
          }.mkString("(", ", ", ")")
        }
        val content = (1.to(num)).map { i =>
          s"k$i) new Context${num - 1}${newCtx(num, i)}"
        }.mkString("\n\t\tif (k eq ", "\n\t\telse if (k eq ", "\n\t\telse this")
        name + content
      }

      def mkSet(num: Int): String = {
        val name = s"def set(k: Key, v: Some[_]): Context = "
        def newCtx(sum: Int, current: Int, last: Boolean): String = {
          (1.to(sum)).map { i =>
            if (i == current && last) s"k, v"
            else if (i == current) s"k$i, v"
            else s"k$i, v$i"
          }.mkString("(", ", ", ")")
        }
        val content = (1.to(num)).map { i =>
          s"k$i) new Context$num${newCtx(num, i, false)}"
        }.mkString("\n\t\tif (k eq ", "\n\t\telse if (k eq ", s"\n\t\telse new Context${num + 1}${newCtx(num + 1, num + 1, true)}")
        name + content
      }

      def main(args: Array[String]) {
        print(2.to(15).map{ i => mkContext(i)}.mkString)
      }
    }
     */
    private final class Context3(
      k1: Key,
      v1: Some[_],
      k2: Key,
      v2: Some[_],
      k3: Key,
      v3: Some[_]
    ) extends Context {

      def get(k: Key): Option[_] =
        if (k eq k1) v1
        else if (k eq k2) v2
        else if (k eq k3) v3
        else None

      def remove(k: Key): Context =
        if (k eq k1) new Context2(k2, v2, k3, v3)
        else if (k eq k2) new Context2(k1, v1, k3, v3)
        else if (k eq k3) new Context2(k1, v1, k2, v2)
        else this

      def set(k: Key, v: Some[_]): Context =
        if (k eq k1) new Context3(k1, v, k2, v2, k3, v3)
        else if (k eq k2) new Context3(k1, v1, k2, v, k3, v3)
        else if (k eq k3) new Context3(k1, v1, k2, v2, k3, v)
        else new Context4(k1, v1, k2, v2, k3, v3, k, v)
    }

    private final class Context4(
      k1: Key,
      v1: Some[_],
      k2: Key,
      v2: Some[_],
      k3: Key,
      v3: Some[_],
      k4: Key,
      v4: Some[_]
    ) extends Context {

      def get(k: Key): Option[_] =
        if (k eq k1) v1
        else if (k eq k2) v2
        else if (k eq k3) v3
        else if (k eq k4) v4
        else None

      def remove(k: Key): Context =
        if (k eq k1) new Context3(k2, v2, k3, v3, k4, v4)
        else if (k eq k2) new Context3(k1, v1, k3, v3, k4, v4)
        else if (k eq k3) new Context3(k1, v1, k2, v2, k4, v4)
        else if (k eq k4) new Context3(k1, v1, k2, v2, k3, v3)
        else this

      def set(k: Key, v: Some[_]): Context =
        if (k eq k1) new Context4(k1, v, k2, v2, k3, v3, k4, v4)
        else if (k eq k2) new Context4(k1, v1, k2, v, k3, v3, k4, v4)
        else if (k eq k3) new Context4(k1, v1, k2, v2, k3, v, k4, v4)
        else if (k eq k4) new Context4(k1, v1, k2, v2, k3, v3, k4, v)
        else new Context5(k1, v1, k2, v2, k3, v3, k4, v4, k, v)
    }

    private final class Context5(
      k1: Key,
      v1: Some[_],
      k2: Key,
      v2: Some[_],
      k3: Key,
      v3: Some[_],
      k4: Key,
      v4: Some[_],
      k5: Key,
      v5: Some[_]
    ) extends Context {

      def get(k: Key): Option[_] =
        if (k eq k1) v1
        else if (k eq k2) v2
        else if (k eq k3) v3
        else if (k eq k4) v4
        else if (k eq k5) v5
        else None

      def remove(k: Key): Context =
        if (k eq k1) new Context4(k2, v2, k3, v3, k4, v4, k5, v5)
        else if (k eq k2) new Context4(k1, v1, k3, v3, k4, v4, k5, v5)
        else if (k eq k3) new Context4(k1, v1, k2, v2, k4, v4, k5, v5)
        else if (k eq k4) new Context4(k1, v1, k2, v2, k3, v3, k5, v5)
        else if (k eq k5) new Context4(k1, v1, k2, v2, k3, v3, k4, v4)
        else this

      def set(k: Key, v: Some[_]): Context =
        if (k eq k1) new Context5(k1, v, k2, v2, k3, v3, k4, v4, k5, v5)
        else if (k eq k2) new Context5(k1, v1, k2, v, k3, v3, k4, v4, k5, v5)
        else if (k eq k3) new Context5(k1, v1, k2, v2, k3, v, k4, v4, k5, v5)
        else if (k eq k4) new Context5(k1, v1, k2, v2, k3, v3, k4, v, k5, v5)
        else if (k eq k5) new Context5(k1, v1, k2, v2, k3, v3, k4, v4, k5, v)
        else new Context6(k1, v1, k2, v2, k3, v3, k4, v4, k5, v5, k, v)
    }

    private final class Context6(
      k1: Key,
      v1: Some[_],
      k2: Key,
      v2: Some[_],
      k3: Key,
      v3: Some[_],
      k4: Key,
      v4: Some[_],
      k5: Key,
      v5: Some[_],
      k6: Key,
      v6: Some[_]
    ) extends Context {

      def get(k: Key): Option[_] =
        if (k eq k1) v1
        else if (k eq k2) v2
        else if (k eq k3) v3
        else if (k eq k4) v4
        else if (k eq k5) v5
        else if (k eq k6) v6
        else None

      def remove(k: Key): Context =
        if (k eq k1) new Context5(k2, v2, k3, v3, k4, v4, k5, v5, k6, v6)
        else if (k eq k2) new Context5(k1, v1, k3, v3, k4, v4, k5, v5, k6, v6)
        else if (k eq k3) new Context5(k1, v1, k2, v2, k4, v4, k5, v5, k6, v6)
        else if (k eq k4) new Context5(k1, v1, k2, v2, k3, v3, k5, v5, k6, v6)
        else if (k eq k5) new Context5(k1, v1, k2, v2, k3, v3, k4, v4, k6, v6)
        else if (k eq k6) new Context5(k1, v1, k2, v2, k3, v3, k4, v4, k5, v5)
        else this

      def set(k: Key, v: Some[_]): Context =
        if (k eq k1) new Context6(k1, v, k2, v2, k3, v3, k4, v4, k5, v5, k6, v6)
        else if (k eq k2) new Context6(k1, v1, k2, v, k3, v3, k4, v4, k5, v5, k6, v6)
        else if (k eq k3) new Context6(k1, v1, k2, v2, k3, v, k4, v4, k5, v5, k6, v6)
        else if (k eq k4) new Context6(k1, v1, k2, v2, k3, v3, k4, v, k5, v5, k6, v6)
        else if (k eq k5) new Context6(k1, v1, k2, v2, k3, v3, k4, v4, k5, v, k6, v6)
        else if (k eq k6) new Context6(k1, v1, k2, v2, k3, v3, k4, v4, k5, v5, k6, v)
        else new Context7(k1, v1, k2, v2, k3, v3, k4, v4, k5, v5, k6, v6, k, v)
    }

    private final class Context7(
      k1: Key,
      v1: Some[_],
      k2: Key,
      v2: Some[_],
      k3: Key,
      v3: Some[_],
      k4: Key,
      v4: Some[_],
      k5: Key,
      v5: Some[_],
      k6: Key,
      v6: Some[_],
      k7: Key,
      v7: Some[_]
    ) extends Context {

      def get(k: Key): Option[_] =
        if (k eq k1) v1
        else if (k eq k2) v2
        else if (k eq k3) v3
        else if (k eq k4) v4
        else if (k eq k5) v5
        else if (k eq k6) v6
        else if (k eq k7) v7
        else None

      def remove(k: Key): Context =
        if (k eq k1) new Context6(k2, v2, k3, v3, k4, v4, k5, v5, k6, v6, k7, v7)
        else if (k eq k2) new Context6(k1, v1, k3, v3, k4, v4, k5, v5, k6, v6, k7, v7)
        else if (k eq k3) new Context6(k1, v1, k2, v2, k4, v4, k5, v5, k6, v6, k7, v7)
        else if (k eq k4) new Context6(k1, v1, k2, v2, k3, v3, k5, v5, k6, v6, k7, v7)
        else if (k eq k5) new Context6(k1, v1, k2, v2, k3, v3, k4, v4, k6, v6, k7, v7)
        else if (k eq k6) new Context6(k1, v1, k2, v2, k3, v3, k4, v4, k5, v5, k7, v7)
        else if (k eq k7) new Context6(k1, v1, k2, v2, k3, v3, k4, v4, k5, v5, k6, v6)
        else this

      def set(k: Key, v: Some[_]): Context =
        if (k eq k1) new Context7(k1, v, k2, v2, k3, v3, k4, v4, k5, v5, k6, v6, k7, v7)
        else if (k eq k2) new Context7(k1, v1, k2, v, k3, v3, k4, v4, k5, v5, k6, v6, k7, v7)
        else if (k eq k3) new Context7(k1, v1, k2, v2, k3, v, k4, v4, k5, v5, k6, v6, k7, v7)
        else if (k eq k4) new Context7(k1, v1, k2, v2, k3, v3, k4, v, k5, v5, k6, v6, k7, v7)
        else if (k eq k5) new Context7(k1, v1, k2, v2, k3, v3, k4, v4, k5, v, k6, v6, k7, v7)
        else if (k eq k6) new Context7(k1, v1, k2, v2, k3, v3, k4, v4, k5, v5, k6, v, k7, v7)
        else if (k eq k7) new Context7(k1, v1, k2, v2, k3, v3, k4, v4, k5, v5, k6, v6, k7, v)
        else new Context8(k1, v1, k2, v2, k3, v3, k4, v4, k5, v5, k6, v6, k7, v7, k, v)
    }

    private final class Context8(
      k1: Key,
      v1: Some[_],
      k2: Key,
      v2: Some[_],
      k3: Key,
      v3: Some[_],
      k4: Key,
      v4: Some[_],
      k5: Key,
      v5: Some[_],
      k6: Key,
      v6: Some[_],
      k7: Key,
      v7: Some[_],
      k8: Key,
      v8: Some[_]
    ) extends Context {

      def get(k: Key): Option[_] =
        if (k eq k1) v1
        else if (k eq k2) v2
        else if (k eq k3) v3
        else if (k eq k4) v4
        else if (k eq k5) v5
        else if (k eq k6) v6
        else if (k eq k7) v7
        else if (k eq k8) v8
        else None

      def remove(k: Key): Context =
        if (k eq k1) new Context7(k2, v2, k3, v3, k4, v4, k5, v5, k6, v6, k7, v7, k8, v8)
        else if (k eq k2) new Context7(k1, v1, k3, v3, k4, v4, k5, v5, k6, v6, k7, v7, k8, v8)
        else if (k eq k3) new Context7(k1, v1, k2, v2, k4, v4, k5, v5, k6, v6, k7, v7, k8, v8)
        else if (k eq k4) new Context7(k1, v1, k2, v2, k3, v3, k5, v5, k6, v6, k7, v7, k8, v8)
        else if (k eq k5) new Context7(k1, v1, k2, v2, k3, v3, k4, v4, k6, v6, k7, v7, k8, v8)
        else if (k eq k6) new Context7(k1, v1, k2, v2, k3, v3, k4, v4, k5, v5, k7, v7, k8, v8)
        else if (k eq k7) new Context7(k1, v1, k2, v2, k3, v3, k4, v4, k5, v5, k6, v6, k8, v8)
        else if (k eq k8) new Context7(k1, v1, k2, v2, k3, v3, k4, v4, k5, v5, k6, v6, k7, v7)
        else this

      def set(k: Key, v: Some[_]): Context =
        if (k eq k1) new Context8(k1, v, k2, v2, k3, v3, k4, v4, k5, v5, k6, v6, k7, v7, k8, v8)
        else if (k eq k2)
          new Context8(k1, v1, k2, v, k3, v3, k4, v4, k5, v5, k6, v6, k7, v7, k8, v8)
        else if (k eq k3)
          new Context8(k1, v1, k2, v2, k3, v, k4, v4, k5, v5, k6, v6, k7, v7, k8, v8)
        else if (k eq k4)
          new Context8(k1, v1, k2, v2, k3, v3, k4, v, k5, v5, k6, v6, k7, v7, k8, v8)
        else if (k eq k5)
          new Context8(k1, v1, k2, v2, k3, v3, k4, v4, k5, v, k6, v6, k7, v7, k8, v8)
        else if (k eq k6)
          new Context8(k1, v1, k2, v2, k3, v3, k4, v4, k5, v5, k6, v, k7, v7, k8, v8)
        else if (k eq k7)
          new Context8(k1, v1, k2, v2, k3, v3, k4, v4, k5, v5, k6, v6, k7, v, k8, v8)
        else if (k eq k8)
          new Context8(k1, v1, k2, v2, k3, v3, k4, v4, k5, v5, k6, v6, k7, v7, k8, v)
        else new Context9(k1, v1, k2, v2, k3, v3, k4, v4, k5, v5, k6, v6, k7, v7, k8, v8, k, v)
    }

    private final class Context9(
      k1: Key,
      v1: Some[_],
      k2: Key,
      v2: Some[_],
      k3: Key,
      v3: Some[_],
      k4: Key,
      v4: Some[_],
      k5: Key,
      v5: Some[_],
      k6: Key,
      v6: Some[_],
      k7: Key,
      v7: Some[_],
      k8: Key,
      v8: Some[_],
      k9: Key,
      v9: Some[_]
    ) extends Context {

      def get(k: Key): Option[_] =
        if (k eq k1) v1
        else if (k eq k2) v2
        else if (k eq k3) v3
        else if (k eq k4) v4
        else if (k eq k5) v5
        else if (k eq k6) v6
        else if (k eq k7) v7
        else if (k eq k8) v8
        else if (k eq k9) v9
        else None

      def remove(k: Key): Context =
        if (k eq k1) new Context8(k2, v2, k3, v3, k4, v4, k5, v5, k6, v6, k7, v7, k8, v8, k9, v9)
        else if (k eq k2)
          new Context8(k1, v1, k3, v3, k4, v4, k5, v5, k6, v6, k7, v7, k8, v8, k9, v9)
        else if (k eq k3)
          new Context8(k1, v1, k2, v2, k4, v4, k5, v5, k6, v6, k7, v7, k8, v8, k9, v9)
        else if (k eq k4)
          new Context8(k1, v1, k2, v2, k3, v3, k5, v5, k6, v6, k7, v7, k8, v8, k9, v9)
        else if (k eq k5)
          new Context8(k1, v1, k2, v2, k3, v3, k4, v4, k6, v6, k7, v7, k8, v8, k9, v9)
        else if (k eq k6)
          new Context8(k1, v1, k2, v2, k3, v3, k4, v4, k5, v5, k7, v7, k8, v8, k9, v9)
        else if (k eq k7)
          new Context8(k1, v1, k2, v2, k3, v3, k4, v4, k5, v5, k6, v6, k8, v8, k9, v9)
        else if (k eq k8)
          new Context8(k1, v1, k2, v2, k3, v3, k4, v4, k5, v5, k6, v6, k7, v7, k9, v9)
        else if (k eq k9)
          new Context8(k1, v1, k2, v2, k3, v3, k4, v4, k5, v5, k6, v6, k7, v7, k8, v8)
        else this

      def set(k: Key, v: Some[_]): Context =
        if (k eq k1)
          new Context9(k1, v, k2, v2, k3, v3, k4, v4, k5, v5, k6, v6, k7, v7, k8, v8, k9, v9)
        else if (k eq k2)
          new Context9(k1, v1, k2, v, k3, v3, k4, v4, k5, v5, k6, v6, k7, v7, k8, v8, k9, v9)
        else if (k eq k3)
          new Context9(k1, v1, k2, v2, k3, v, k4, v4, k5, v5, k6, v6, k7, v7, k8, v8, k9, v9)
        else if (k eq k4)
          new Context9(k1, v1, k2, v2, k3, v3, k4, v, k5, v5, k6, v6, k7, v7, k8, v8, k9, v9)
        else if (k eq k5)
          new Context9(k1, v1, k2, v2, k3, v3, k4, v4, k5, v, k6, v6, k7, v7, k8, v8, k9, v9)
        else if (k eq k6)
          new Context9(k1, v1, k2, v2, k3, v3, k4, v4, k5, v5, k6, v, k7, v7, k8, v8, k9, v9)
        else if (k eq k7)
          new Context9(k1, v1, k2, v2, k3, v3, k4, v4, k5, v5, k6, v6, k7, v, k8, v8, k9, v9)
        else if (k eq k8)
          new Context9(k1, v1, k2, v2, k3, v3, k4, v4, k5, v5, k6, v6, k7, v7, k8, v, k9, v9)
        else if (k eq k9)
          new Context9(k1, v1, k2, v2, k3, v3, k4, v4, k5, v5, k6, v6, k7, v7, k8, v8, k9, v)
        else
          new Context10(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k,
            v
          )
    }

    private final class Context10(
      k1: Key,
      v1: Some[_],
      k2: Key,
      v2: Some[_],
      k3: Key,
      v3: Some[_],
      k4: Key,
      v4: Some[_],
      k5: Key,
      v5: Some[_],
      k6: Key,
      v6: Some[_],
      k7: Key,
      v7: Some[_],
      k8: Key,
      v8: Some[_],
      k9: Key,
      v9: Some[_],
      k10: Key,
      v10: Some[_]
    ) extends Context {

      def get(k: Key): Option[_] =
        if (k eq k1) v1
        else if (k eq k2) v2
        else if (k eq k3) v3
        else if (k eq k4) v4
        else if (k eq k5) v5
        else if (k eq k6) v6
        else if (k eq k7) v7
        else if (k eq k8) v8
        else if (k eq k9) v9
        else if (k eq k10) v10
        else None

      def remove(k: Key): Context =
        if (k eq k1)
          new Context9(k2, v2, k3, v3, k4, v4, k5, v5, k6, v6, k7, v7, k8, v8, k9, v9, k10, v10)
        else if (k eq k2)
          new Context9(k1, v1, k3, v3, k4, v4, k5, v5, k6, v6, k7, v7, k8, v8, k9, v9, k10, v10)
        else if (k eq k3)
          new Context9(k1, v1, k2, v2, k4, v4, k5, v5, k6, v6, k7, v7, k8, v8, k9, v9, k10, v10)
        else if (k eq k4)
          new Context9(k1, v1, k2, v2, k3, v3, k5, v5, k6, v6, k7, v7, k8, v8, k9, v9, k10, v10)
        else if (k eq k5)
          new Context9(k1, v1, k2, v2, k3, v3, k4, v4, k6, v6, k7, v7, k8, v8, k9, v9, k10, v10)
        else if (k eq k6)
          new Context9(k1, v1, k2, v2, k3, v3, k4, v4, k5, v5, k7, v7, k8, v8, k9, v9, k10, v10)
        else if (k eq k7)
          new Context9(k1, v1, k2, v2, k3, v3, k4, v4, k5, v5, k6, v6, k8, v8, k9, v9, k10, v10)
        else if (k eq k8)
          new Context9(k1, v1, k2, v2, k3, v3, k4, v4, k5, v5, k6, v6, k7, v7, k9, v9, k10, v10)
        else if (k eq k9)
          new Context9(k1, v1, k2, v2, k3, v3, k4, v4, k5, v5, k6, v6, k7, v7, k8, v8, k10, v10)
        else if (k eq k10)
          new Context9(k1, v1, k2, v2, k3, v3, k4, v4, k5, v5, k6, v6, k7, v7, k8, v8, k9, v9)
        else this

      def set(k: Key, v: Some[_]): Context =
        if (k eq k1)
          new Context10(
            k1,
            v,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10
          )
        else if (k eq k2)
          new Context10(
            k1,
            v1,
            k2,
            v,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10
          )
        else if (k eq k3)
          new Context10(
            k1,
            v1,
            k2,
            v2,
            k3,
            v,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10
          )
        else if (k eq k4)
          new Context10(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10
          )
        else if (k eq k5)
          new Context10(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10
          )
        else if (k eq k6)
          new Context10(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10
          )
        else if (k eq k7)
          new Context10(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10
          )
        else if (k eq k8)
          new Context10(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v,
            k9,
            v9,
            k10,
            v10
          )
        else if (k eq k9)
          new Context10(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v,
            k10,
            v10
          )
        else if (k eq k10)
          new Context10(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v
          )
        else
          new Context11(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k,
            v
          )
    }

    private final class Context11(
      k1: Key,
      v1: Some[_],
      k2: Key,
      v2: Some[_],
      k3: Key,
      v3: Some[_],
      k4: Key,
      v4: Some[_],
      k5: Key,
      v5: Some[_],
      k6: Key,
      v6: Some[_],
      k7: Key,
      v7: Some[_],
      k8: Key,
      v8: Some[_],
      k9: Key,
      v9: Some[_],
      k10: Key,
      v10: Some[_],
      k11: Key,
      v11: Some[_]
    ) extends Context {

      def get(k: Key): Option[_] =
        if (k eq k1) v1
        else if (k eq k2) v2
        else if (k eq k3) v3
        else if (k eq k4) v4
        else if (k eq k5) v5
        else if (k eq k6) v6
        else if (k eq k7) v7
        else if (k eq k8) v8
        else if (k eq k9) v9
        else if (k eq k10) v10
        else if (k eq k11) v11
        else None

      def remove(k: Key): Context =
        if (k eq k1)
          new Context10(
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11
          )
        else if (k eq k2)
          new Context10(
            k1,
            v1,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11
          )
        else if (k eq k3)
          new Context10(
            k1,
            v1,
            k2,
            v2,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11
          )
        else if (k eq k4)
          new Context10(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11
          )
        else if (k eq k5)
          new Context10(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11
          )
        else if (k eq k6)
          new Context10(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11
          )
        else if (k eq k7)
          new Context10(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11
          )
        else if (k eq k8)
          new Context10(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11
          )
        else if (k eq k9)
          new Context10(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k10,
            v10,
            k11,
            v11
          )
        else if (k eq k10)
          new Context10(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k11,
            v11
          )
        else if (k eq k11)
          new Context10(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10
          )
        else this

      def set(k: Key, v: Some[_]): Context =
        if (k eq k1)
          new Context11(
            k1,
            v,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11
          )
        else if (k eq k2)
          new Context11(
            k1,
            v1,
            k2,
            v,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11
          )
        else if (k eq k3)
          new Context11(
            k1,
            v1,
            k2,
            v2,
            k3,
            v,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11
          )
        else if (k eq k4)
          new Context11(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11
          )
        else if (k eq k5)
          new Context11(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11
          )
        else if (k eq k6)
          new Context11(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11
          )
        else if (k eq k7)
          new Context11(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11
          )
        else if (k eq k8)
          new Context11(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11
          )
        else if (k eq k9)
          new Context11(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v,
            k10,
            v10,
            k11,
            v11
          )
        else if (k eq k10)
          new Context11(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v,
            k11,
            v11
          )
        else if (k eq k11)
          new Context11(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v
          )
        else
          new Context12(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11,
            k,
            v
          )
    }

    private final class Context12(
      k1: Key,
      v1: Some[_],
      k2: Key,
      v2: Some[_],
      k3: Key,
      v3: Some[_],
      k4: Key,
      v4: Some[_],
      k5: Key,
      v5: Some[_],
      k6: Key,
      v6: Some[_],
      k7: Key,
      v7: Some[_],
      k8: Key,
      v8: Some[_],
      k9: Key,
      v9: Some[_],
      k10: Key,
      v10: Some[_],
      k11: Key,
      v11: Some[_],
      k12: Key,
      v12: Some[_]
    ) extends Context {

      def get(k: Key): Option[_] =
        if (k eq k1) v1
        else if (k eq k2) v2
        else if (k eq k3) v3
        else if (k eq k4) v4
        else if (k eq k5) v5
        else if (k eq k6) v6
        else if (k eq k7) v7
        else if (k eq k8) v8
        else if (k eq k9) v9
        else if (k eq k10) v10
        else if (k eq k11) v11
        else if (k eq k12) v12
        else None

      def remove(k: Key): Context =
        if (k eq k1)
          new Context11(
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11,
            k12,
            v12
          )
        else if (k eq k2)
          new Context11(
            k1,
            v1,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11,
            k12,
            v12
          )
        else if (k eq k3)
          new Context11(
            k1,
            v1,
            k2,
            v2,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11,
            k12,
            v12
          )
        else if (k eq k4)
          new Context11(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11,
            k12,
            v12
          )
        else if (k eq k5)
          new Context11(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11,
            k12,
            v12
          )
        else if (k eq k6)
          new Context11(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11,
            k12,
            v12
          )
        else if (k eq k7)
          new Context11(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11,
            k12,
            v12
          )
        else if (k eq k8)
          new Context11(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11,
            k12,
            v12
          )
        else if (k eq k9)
          new Context11(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k10,
            v10,
            k11,
            v11,
            k12,
            v12
          )
        else if (k eq k10)
          new Context11(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k11,
            v11,
            k12,
            v12
          )
        else if (k eq k11)
          new Context11(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k12,
            v12
          )
        else if (k eq k12)
          new Context11(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11
          )
        else this

      def set(k: Key, v: Some[_]): Context =
        if (k eq k1)
          new Context12(
            k1,
            v,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11,
            k12,
            v12
          )
        else if (k eq k2)
          new Context12(
            k1,
            v1,
            k2,
            v,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11,
            k12,
            v12
          )
        else if (k eq k3)
          new Context12(
            k1,
            v1,
            k2,
            v2,
            k3,
            v,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11,
            k12,
            v12
          )
        else if (k eq k4)
          new Context12(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11,
            k12,
            v12
          )
        else if (k eq k5)
          new Context12(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11,
            k12,
            v12
          )
        else if (k eq k6)
          new Context12(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11,
            k12,
            v12
          )
        else if (k eq k7)
          new Context12(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11,
            k12,
            v12
          )
        else if (k eq k8)
          new Context12(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11,
            k12,
            v12
          )
        else if (k eq k9)
          new Context12(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v,
            k10,
            v10,
            k11,
            v11,
            k12,
            v12
          )
        else if (k eq k10)
          new Context12(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v,
            k11,
            v11,
            k12,
            v12
          )
        else if (k eq k11)
          new Context12(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v,
            k12,
            v12
          )
        else if (k eq k12)
          new Context12(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11,
            k12,
            v
          )
        else
          new Context13(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11,
            k12,
            v12,
            k,
            v
          )
    }

    private final class Context13(
      k1: Key,
      v1: Some[_],
      k2: Key,
      v2: Some[_],
      k3: Key,
      v3: Some[_],
      k4: Key,
      v4: Some[_],
      k5: Key,
      v5: Some[_],
      k6: Key,
      v6: Some[_],
      k7: Key,
      v7: Some[_],
      k8: Key,
      v8: Some[_],
      k9: Key,
      v9: Some[_],
      k10: Key,
      v10: Some[_],
      k11: Key,
      v11: Some[_],
      k12: Key,
      v12: Some[_],
      k13: Key,
      v13: Some[_]
    ) extends Context {

      def get(k: Key): Option[_] =
        if (k eq k1) v1
        else if (k eq k2) v2
        else if (k eq k3) v3
        else if (k eq k4) v4
        else if (k eq k5) v5
        else if (k eq k6) v6
        else if (k eq k7) v7
        else if (k eq k8) v8
        else if (k eq k9) v9
        else if (k eq k10) v10
        else if (k eq k11) v11
        else if (k eq k12) v12
        else if (k eq k13) v13
        else None

      def remove(k: Key): Context =
        if (k eq k1)
          new Context12(
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11,
            k12,
            v12,
            k13,
            v13
          )
        else if (k eq k2)
          new Context12(
            k1,
            v1,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11,
            k12,
            v12,
            k13,
            v13
          )
        else if (k eq k3)
          new Context12(
            k1,
            v1,
            k2,
            v2,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11,
            k12,
            v12,
            k13,
            v13
          )
        else if (k eq k4)
          new Context12(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11,
            k12,
            v12,
            k13,
            v13
          )
        else if (k eq k5)
          new Context12(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11,
            k12,
            v12,
            k13,
            v13
          )
        else if (k eq k6)
          new Context12(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11,
            k12,
            v12,
            k13,
            v13
          )
        else if (k eq k7)
          new Context12(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11,
            k12,
            v12,
            k13,
            v13
          )
        else if (k eq k8)
          new Context12(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11,
            k12,
            v12,
            k13,
            v13
          )
        else if (k eq k9)
          new Context12(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k10,
            v10,
            k11,
            v11,
            k12,
            v12,
            k13,
            v13
          )
        else if (k eq k10)
          new Context12(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k11,
            v11,
            k12,
            v12,
            k13,
            v13
          )
        else if (k eq k11)
          new Context12(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k12,
            v12,
            k13,
            v13
          )
        else if (k eq k12)
          new Context12(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11,
            k13,
            v13
          )
        else if (k eq k13)
          new Context12(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11,
            k12,
            v12
          )
        else this

      def set(k: Key, v: Some[_]): Context =
        if (k eq k1)
          new Context13(
            k1,
            v,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11,
            k12,
            v12,
            k13,
            v13
          )
        else if (k eq k2)
          new Context13(
            k1,
            v1,
            k2,
            v,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11,
            k12,
            v12,
            k13,
            v13
          )
        else if (k eq k3)
          new Context13(
            k1,
            v1,
            k2,
            v2,
            k3,
            v,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11,
            k12,
            v12,
            k13,
            v13
          )
        else if (k eq k4)
          new Context13(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11,
            k12,
            v12,
            k13,
            v13
          )
        else if (k eq k5)
          new Context13(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11,
            k12,
            v12,
            k13,
            v13
          )
        else if (k eq k6)
          new Context13(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11,
            k12,
            v12,
            k13,
            v13
          )
        else if (k eq k7)
          new Context13(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11,
            k12,
            v12,
            k13,
            v13
          )
        else if (k eq k8)
          new Context13(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11,
            k12,
            v12,
            k13,
            v13
          )
        else if (k eq k9)
          new Context13(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v,
            k10,
            v10,
            k11,
            v11,
            k12,
            v12,
            k13,
            v13
          )
        else if (k eq k10)
          new Context13(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v,
            k11,
            v11,
            k12,
            v12,
            k13,
            v13
          )
        else if (k eq k11)
          new Context13(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v,
            k12,
            v12,
            k13,
            v13
          )
        else if (k eq k12)
          new Context13(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11,
            k12,
            v,
            k13,
            v13
          )
        else if (k eq k13)
          new Context13(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11,
            k12,
            v12,
            k13,
            v
          )
        else
          new Context14(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11,
            k12,
            v12,
            k13,
            v13,
            k,
            v
          )
    }

    private final class Context14(
      k1: Key,
      v1: Some[_],
      k2: Key,
      v2: Some[_],
      k3: Key,
      v3: Some[_],
      k4: Key,
      v4: Some[_],
      k5: Key,
      v5: Some[_],
      k6: Key,
      v6: Some[_],
      k7: Key,
      v7: Some[_],
      k8: Key,
      v8: Some[_],
      k9: Key,
      v9: Some[_],
      k10: Key,
      v10: Some[_],
      k11: Key,
      v11: Some[_],
      k12: Key,
      v12: Some[_],
      k13: Key,
      v13: Some[_],
      k14: Key,
      v14: Some[_]
    ) extends Context {

      def get(k: Key): Option[_] =
        if (k eq k1) v1
        else if (k eq k2) v2
        else if (k eq k3) v3
        else if (k eq k4) v4
        else if (k eq k5) v5
        else if (k eq k6) v6
        else if (k eq k7) v7
        else if (k eq k8) v8
        else if (k eq k9) v9
        else if (k eq k10) v10
        else if (k eq k11) v11
        else if (k eq k12) v12
        else if (k eq k13) v13
        else if (k eq k14) v14
        else None

      def remove(k: Key): Context =
        if (k eq k1)
          new Context13(
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11,
            k12,
            v12,
            k13,
            v13,
            k14,
            v14
          )
        else if (k eq k2)
          new Context13(
            k1,
            v1,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11,
            k12,
            v12,
            k13,
            v13,
            k14,
            v14
          )
        else if (k eq k3)
          new Context13(
            k1,
            v1,
            k2,
            v2,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11,
            k12,
            v12,
            k13,
            v13,
            k14,
            v14
          )
        else if (k eq k4)
          new Context13(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11,
            k12,
            v12,
            k13,
            v13,
            k14,
            v14
          )
        else if (k eq k5)
          new Context13(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11,
            k12,
            v12,
            k13,
            v13,
            k14,
            v14
          )
        else if (k eq k6)
          new Context13(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11,
            k12,
            v12,
            k13,
            v13,
            k14,
            v14
          )
        else if (k eq k7)
          new Context13(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11,
            k12,
            v12,
            k13,
            v13,
            k14,
            v14
          )
        else if (k eq k8)
          new Context13(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11,
            k12,
            v12,
            k13,
            v13,
            k14,
            v14
          )
        else if (k eq k9)
          new Context13(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k10,
            v10,
            k11,
            v11,
            k12,
            v12,
            k13,
            v13,
            k14,
            v14
          )
        else if (k eq k10)
          new Context13(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k11,
            v11,
            k12,
            v12,
            k13,
            v13,
            k14,
            v14
          )
        else if (k eq k11)
          new Context13(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k12,
            v12,
            k13,
            v13,
            k14,
            v14
          )
        else if (k eq k12)
          new Context13(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11,
            k13,
            v13,
            k14,
            v14
          )
        else if (k eq k13)
          new Context13(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11,
            k12,
            v12,
            k14,
            v14
          )
        else if (k eq k14)
          new Context13(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11,
            k12,
            v12,
            k13,
            v13
          )
        else this

      def set(k: Key, v: Some[_]): Context =
        if (k eq k1)
          new Context14(
            k1,
            v,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11,
            k12,
            v12,
            k13,
            v13,
            k14,
            v14
          )
        else if (k eq k2)
          new Context14(
            k1,
            v1,
            k2,
            v,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11,
            k12,
            v12,
            k13,
            v13,
            k14,
            v14
          )
        else if (k eq k3)
          new Context14(
            k1,
            v1,
            k2,
            v2,
            k3,
            v,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11,
            k12,
            v12,
            k13,
            v13,
            k14,
            v14
          )
        else if (k eq k4)
          new Context14(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11,
            k12,
            v12,
            k13,
            v13,
            k14,
            v14
          )
        else if (k eq k5)
          new Context14(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11,
            k12,
            v12,
            k13,
            v13,
            k14,
            v14
          )
        else if (k eq k6)
          new Context14(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11,
            k12,
            v12,
            k13,
            v13,
            k14,
            v14
          )
        else if (k eq k7)
          new Context14(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11,
            k12,
            v12,
            k13,
            v13,
            k14,
            v14
          )
        else if (k eq k8)
          new Context14(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11,
            k12,
            v12,
            k13,
            v13,
            k14,
            v14
          )
        else if (k eq k9)
          new Context14(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v,
            k10,
            v10,
            k11,
            v11,
            k12,
            v12,
            k13,
            v13,
            k14,
            v14
          )
        else if (k eq k10)
          new Context14(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v,
            k11,
            v11,
            k12,
            v12,
            k13,
            v13,
            k14,
            v14
          )
        else if (k eq k11)
          new Context14(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v,
            k12,
            v12,
            k13,
            v13,
            k14,
            v14
          )
        else if (k eq k12)
          new Context14(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11,
            k12,
            v,
            k13,
            v13,
            k14,
            v14
          )
        else if (k eq k13)
          new Context14(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11,
            k12,
            v12,
            k13,
            v,
            k14,
            v14
          )
        else if (k eq k14)
          new Context14(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11,
            k12,
            v12,
            k13,
            v13,
            k14,
            v
          )
        else
          new Context15(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11,
            k12,
            v12,
            k13,
            v13,
            k14,
            v14,
            k,
            v
          )
    }

    private final class Context15(
      k1: Key,
      v1: Some[_],
      k2: Key,
      v2: Some[_],
      k3: Key,
      v3: Some[_],
      k4: Key,
      v4: Some[_],
      k5: Key,
      v5: Some[_],
      k6: Key,
      v6: Some[_],
      k7: Key,
      v7: Some[_],
      k8: Key,
      v8: Some[_],
      k9: Key,
      v9: Some[_],
      k10: Key,
      v10: Some[_],
      k11: Key,
      v11: Some[_],
      k12: Key,
      v12: Some[_],
      k13: Key,
      v13: Some[_],
      k14: Key,
      v14: Some[_],
      k15: Key,
      v15: Some[_]
    ) extends Context {

      def get(k: Key): Option[_] =
        if (k eq k1) v1
        else if (k eq k2) v2
        else if (k eq k3) v3
        else if (k eq k4) v4
        else if (k eq k5) v5
        else if (k eq k6) v6
        else if (k eq k7) v7
        else if (k eq k8) v8
        else if (k eq k9) v9
        else if (k eq k10) v10
        else if (k eq k11) v11
        else if (k eq k12) v12
        else if (k eq k13) v13
        else if (k eq k14) v14
        else if (k eq k15) v15
        else None

      def remove(k: Key): Context =
        if (k eq k1)
          new Context14(
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11,
            k12,
            v12,
            k13,
            v13,
            k14,
            v14,
            k15,
            v15
          )
        else if (k eq k2)
          new Context14(
            k1,
            v1,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11,
            k12,
            v12,
            k13,
            v13,
            k14,
            v14,
            k15,
            v15
          )
        else if (k eq k3)
          new Context14(
            k1,
            v1,
            k2,
            v2,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11,
            k12,
            v12,
            k13,
            v13,
            k14,
            v14,
            k15,
            v15
          )
        else if (k eq k4)
          new Context14(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11,
            k12,
            v12,
            k13,
            v13,
            k14,
            v14,
            k15,
            v15
          )
        else if (k eq k5)
          new Context14(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11,
            k12,
            v12,
            k13,
            v13,
            k14,
            v14,
            k15,
            v15
          )
        else if (k eq k6)
          new Context14(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11,
            k12,
            v12,
            k13,
            v13,
            k14,
            v14,
            k15,
            v15
          )
        else if (k eq k7)
          new Context14(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11,
            k12,
            v12,
            k13,
            v13,
            k14,
            v14,
            k15,
            v15
          )
        else if (k eq k8)
          new Context14(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11,
            k12,
            v12,
            k13,
            v13,
            k14,
            v14,
            k15,
            v15
          )
        else if (k eq k9)
          new Context14(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k10,
            v10,
            k11,
            v11,
            k12,
            v12,
            k13,
            v13,
            k14,
            v14,
            k15,
            v15
          )
        else if (k eq k10)
          new Context14(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k11,
            v11,
            k12,
            v12,
            k13,
            v13,
            k14,
            v14,
            k15,
            v15
          )
        else if (k eq k11)
          new Context14(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k12,
            v12,
            k13,
            v13,
            k14,
            v14,
            k15,
            v15
          )
        else if (k eq k12)
          new Context14(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11,
            k13,
            v13,
            k14,
            v14,
            k15,
            v15
          )
        else if (k eq k13)
          new Context14(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11,
            k12,
            v12,
            k14,
            v14,
            k15,
            v15
          )
        else if (k eq k14)
          new Context14(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11,
            k12,
            v12,
            k13,
            v13,
            k15,
            v15
          )
        else if (k eq k15)
          new Context14(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11,
            k12,
            v12,
            k13,
            v13,
            k14,
            v14
          )
        else this

      def set(k: Key, v: Some[_]): Context =
        if (k eq k1)
          new Context15(
            k1,
            v,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11,
            k12,
            v12,
            k13,
            v13,
            k14,
            v14,
            k15,
            v15
          )
        else if (k eq k2)
          new Context15(
            k1,
            v1,
            k2,
            v,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11,
            k12,
            v12,
            k13,
            v13,
            k14,
            v14,
            k15,
            v15
          )
        else if (k eq k3)
          new Context15(
            k1,
            v1,
            k2,
            v2,
            k3,
            v,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11,
            k12,
            v12,
            k13,
            v13,
            k14,
            v14,
            k15,
            v15
          )
        else if (k eq k4)
          new Context15(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11,
            k12,
            v12,
            k13,
            v13,
            k14,
            v14,
            k15,
            v15
          )
        else if (k eq k5)
          new Context15(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11,
            k12,
            v12,
            k13,
            v13,
            k14,
            v14,
            k15,
            v15
          )
        else if (k eq k6)
          new Context15(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11,
            k12,
            v12,
            k13,
            v13,
            k14,
            v14,
            k15,
            v15
          )
        else if (k eq k7)
          new Context15(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11,
            k12,
            v12,
            k13,
            v13,
            k14,
            v14,
            k15,
            v15
          )
        else if (k eq k8)
          new Context15(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11,
            k12,
            v12,
            k13,
            v13,
            k14,
            v14,
            k15,
            v15
          )
        else if (k eq k9)
          new Context15(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v,
            k10,
            v10,
            k11,
            v11,
            k12,
            v12,
            k13,
            v13,
            k14,
            v14,
            k15,
            v15
          )
        else if (k eq k10)
          new Context15(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v,
            k11,
            v11,
            k12,
            v12,
            k13,
            v13,
            k14,
            v14,
            k15,
            v15
          )
        else if (k eq k11)
          new Context15(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v,
            k12,
            v12,
            k13,
            v13,
            k14,
            v14,
            k15,
            v15
          )
        else if (k eq k12)
          new Context15(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11,
            k12,
            v,
            k13,
            v13,
            k14,
            v14,
            k15,
            v15
          )
        else if (k eq k13)
          new Context15(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11,
            k12,
            v12,
            k13,
            v,
            k14,
            v14,
            k15,
            v15
          )
        else if (k eq k14)
          new Context15(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11,
            k12,
            v12,
            k13,
            v13,
            k14,
            v,
            k15,
            v15
          )
        else if (k eq k15)
          new Context15(
            k1,
            v1,
            k2,
            v2,
            k3,
            v3,
            k4,
            v4,
            k5,
            v5,
            k6,
            v6,
            k7,
            v7,
            k8,
            v8,
            k9,
            v9,
            k10,
            v10,
            k11,
            v11,
            k12,
            v12,
            k13,
            v13,
            k14,
            v14,
            k15,
            v
          )
        else new ContextN(k, v, this)
    }

    private final class ContextN(
      kN: Key,
      vN: Some[_],
      rest: Context
    ) extends Context {

      def get(k: Key): Option[_] =
        if (k eq kN) vN
        else rest.get(k)

      def remove(k: Key): Context =
        if (k eq kN) rest
        else if (rest.remove(k) eq rest) this
        else new ContextN(kN, vN, rest.remove(k))

      def set(k: Key, v: Some[_]): Context =
        if (k eq kN) new ContextN(kN, v, rest)
        else new ContextN(kN, vN, rest.set(k, v))
    }
  }

  /**
   * Key used in [[Context]], internal.
   */
  private[util] final class Key

  /**
   * Represents the current state of all [[Local locals]] for a given
   * execution context.
   *
   * This should be treated as an opaque value and direct modifications
   * and access are considered verboten.
   */
  private[this] val localCtx = new ThreadLocal[Context] {
    override def initialValue(): Context = Context.empty
  }

  /**
   * Return a snapshot of the current Local state.
   */
  def save(): Context = localCtx.get

  /**
   * Restore the Local state to a given Context of values.
   */
  def restore(saved: Context): Unit = localCtx.set(saved)

  private def set(key: Key, value: Some[_]): Unit = {
    localCtx.set(localCtx.get.set(key, value))
  }

  private def get(key: Key): Option[_] =
    localCtx.get.get(key)

  private def clear(key: Key): Unit =
    localCtx.set(localCtx.get.remove(key))

  /**
   * Clear all locals in the current context.
   */
  def clear(): Unit = localCtx.set(Context.empty)

  /**
   * Execute a block with the given Locals, restoring current values upon completion.
   */
  def let[U](ctx: Context)(f: => U): U = {
    val saved = save()
    restore(ctx)
    try f
    finally restore(saved)
  }

  /**
   * Execute a block with all Locals clear, restoring
   * current values upon completion.
   */
  def letClear[U](f: => U): U = let(Context.empty)(f)

  /**
   * Convert a closure `() => R` into another closure of the same
   * type whose Local context is saved when calling `closed`
   * and restored upon invocation.
   */
  def closed[R](fn: () => R): () => R = {
    val closure = Local.save()
    () =>
      {
        val save = Local.save()
        Local.restore(closure)
        try fn()
        finally Local.restore(save)
      }
  }
}

/**
 * A Local is a [[ThreadLocal]] whose scope is flexible. The state of all Locals may
 * be saved or restored onto the current thread by the user. This is useful for
 * threading Locals through execution contexts.
 *
 * Promises pass locals through control dependencies, not through data
 * dependencies.  This means that Locals have exactly the same semantics as
 * ThreadLocals, if you think of `continue` (the asynchronous sequence operator)
 * as semicolon (the synchronous sequence operator).
 *
 * Because it's not meaningful to inherit control from two places, Locals don't
 * have to worry about having to merge two [[com.twitter.util.Local.Context Contexts]].
 *
 * Note: the implementation is optimized for situations in which save and
 * restore optimizations are dominant.
 */
final class Local[T] {
  private[this] val key = new Local.Key

  /**
   * Update the Local with a given value.
   *
   * General usage should be via [[let]] to avoid leaks.
   */
  def update(value: T): Unit = Local.set(key, Some(value))

  /**
   * Update the Local with a given optional value.
   *
   * General usage should be via [[let]] to avoid leaks.
   */
  def set(optValue: Option[T]): Unit = optValue match {
    case s @ Some(_) => Local.set(key, s)
    case None => Local.clear(key)
  }

  /**
   * Get the Local's optional value.
   */
  def apply(): Option[T] = Local.get(key).asInstanceOf[Option[T]]

  /**
   * Execute a block with a specific Local value, restoring the current state
   * upon completion.
   */
  def let[U](value: T)(f: => U): U = {
    val saved = apply()
    update(value)
    try f
    finally set(saved)
  }

  /**
   * Execute a block with the Local cleared, restoring the current state upon
   * completion.
   */
  def letClear[U](f: => U): U = {
    val saved = apply()
    clear()
    try f
    finally set(saved)
  }

  /**
   * Clear the Local's value. Other [[Local Locals]] are not modified.
   *
   * General usage should be via [[letClear]] to avoid leaks.
   */
  def clear(): Unit = Local.clear(key)
}
