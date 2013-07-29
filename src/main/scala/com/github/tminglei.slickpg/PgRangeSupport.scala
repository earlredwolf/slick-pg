package com.github.tminglei.slickpg

import java.sql.{Date, Timestamp}
import scala.slick.driver.PostgresDriver
import scala.slick.lifted._
import FunctionSymbolExtensionMethods._
import scala.slick.ast.Library.SqlOperator
import scala.slick.ast.{Library, Node}
import org.postgresql.util.PGobject
import scala.slick.jdbc.{PositionedResult, PositionedParameters, JdbcType}

trait PgRangeSupport extends ImplicitJdbcTypes { driver: PostgresDriver =>

  private val tsFormatter = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  private val dateFormatter = new java.text.SimpleDateFormat("yyyy-MM-dd")
  private def toTimestamp(str: String) = new Timestamp(tsFormatter.parse(str).getTime)
  private def toSQLDate(str: String) = new Date(dateFormatter.parse(str).getTime)

  trait RangeImplicits {
    implicit val intRangeJdbcType = new RangeJdbcType[Int]("int4range", Range.mkParser(_.toInt))
    implicit val longRangeJdbcType = new RangeJdbcType[Long]("int8range", Range.mkParser(_.toLong))
    implicit val floatRangeJdbcType = new RangeJdbcType[Float]("numrange", Range.mkParser(_.toFloat))
    implicit val timestampRangeJdbcType = new RangeJdbcType[Timestamp]("tsrange", Range.mkParser(toTimestamp))
    implicit val dateRangeJdbcType = new RangeJdbcType[Date]("daterange", Range.mkParser(toSQLDate))

    implicit def rangeColumnExtensionMethods[B0](c: Column[Range[B0]])(
      implicit tm: JdbcType[B0], tm1: RangeJdbcType[B0]) = {
        new RangeColumnExtensionMethods[B0, Range[B0]](c)
      }
    implicit def rangeOptionColumnExtensionMethods[B0](c: Column[Option[Range[B0]]])(
      implicit tm: JdbcType[B0], tm1: RangeJdbcType[B0]) = {
        new RangeColumnExtensionMethods[B0, Option[Range[B0]]](c)
      }
  }

  ////////////////////////////////////////////////////////////////////////////////

  object RangeLibrary {
    val Contains = new SqlOperator("@>")
    val ContainedBy = new SqlOperator("<@")
    val Overlap = new SqlOperator("&&")
    val StrictLeft = new SqlOperator("<<")
    val StrictRight = new SqlOperator(">>")
    val NotExtendRight = new SqlOperator("&<")
    val NotExtendLeft = new SqlOperator("&>")
    val Adjacent = new SqlOperator("-|-")

    val Union = new SqlOperator("+")
    val Intersection = new SqlOperator("*")
    val Subtraction = new SqlOperator("-")
  }

  class RangeColumnExtensionMethods[B0, P1](val c: Column[P1])(
              implicit tm: JdbcType[B0], tm1: JdbcType[Range[B0]]) extends ExtensionMethods[Range[B0], P1] {

    def @>^[P2, R](e: Column[P2])(implicit om: o#arg[B0, P2]#to[Boolean, R]) = {
        om.column(RangeLibrary.Contains, n, Node(Library.Cast.column[B0](e.nodeDelegate)))
      }
    def @>[P2, R](e: Column[P2])(implicit om: o#arg[Range[B0], P2]#to[Boolean, R]) = {
        om.column(RangeLibrary.Contains, n, Node(e))
      }
    def <@^:[P2, R](e: Column[P2])(implicit om: o#arg[B0, P2]#to[Boolean, R]) = {
        om.column(RangeLibrary.ContainedBy, Node(Library.Cast.column[B0](e.nodeDelegate)), n)
      }
    def <@:[P2, R](e: Column[P2])(implicit om: o#arg[Range[B0], P2]#to[Boolean, R]) = {
        om.column(RangeLibrary.ContainedBy, Node(e), n)
      }
    def @&[P2, R](e: Column[P2])(implicit om: o#arg[Range[B0], P2]#to[Boolean, R]) = {
        om.column(RangeLibrary.Overlap, n, Node(e))
      }
    def <<[P2, R](e: Column[P2])(implicit om: o#arg[Range[B0], P2]#to[Boolean, R]) = {
        om.column(RangeLibrary.StrictLeft, n, Node(e))
      }
    def >>[P2, R](e: Column[P2])(implicit om: o#arg[Range[B0], P2]#to[Boolean, R]) = {
        om.column(RangeLibrary.StrictRight, n, Node(e))
      }
    def &<[P2, R](e: Column[P2])(implicit om: o#arg[Range[B0], P2]#to[Boolean, R]) = {
        om.column(RangeLibrary.NotExtendRight, n, Node(e))
      }
    def &>[P2, R](e: Column[P2])(implicit om: o#arg[Range[B0], P2]#to[Boolean, R]) = {
        om.column(RangeLibrary.NotExtendLeft, n, Node(e))
      }
    def -|-[P2, R](e: Column[P2])(implicit om: o#arg[Range[B0], P2]#to[Boolean, R]) = {
        om.column(RangeLibrary.Adjacent, n, Node(e))
      }

    def + [P2, R](e: Column[P2])(implicit om: o#arg[Range[B0], P2]#to[Range[B0], R]) = {
        om.column(RangeLibrary.Union, n, Node(e))
      }
    def * [P2, R](e: Column[P2])(implicit om: o#arg[Range[B0], P2]#to[Range[B0], R]) = {
        om.column(RangeLibrary.Intersection, n, Node(e))
      }
    def - [P2, R](e: Column[P2])(implicit om: o#arg[Range[B0], P2]#to[Range[B0], R]) = {
        om.column(RangeLibrary.Subtraction, n, Node(e))
      }
  }

  ///////////////////////////////////////////////////////////////////////////////

  class RangeJdbcType[T](rangeType: String, parser: (String => Range[T])) extends DriverJdbcType[Range[T]] {

    def zero: Range[T] = null.asInstanceOf[Range[T]]

    def sqlType: Int = java.sql.Types.OTHER

    override def sqlTypeName: String = rangeType

    def setValue(v: Range[T], p: PositionedParameters) = p.setObject(mkPgObject(v), sqlType)

    def setOption(v: Option[Range[T]], p: PositionedParameters) = p.setObjectOption(v.map(mkPgObject), sqlType)

    def nextValue(r: PositionedResult): Range[T] = r.nextStringOption().map(parser).getOrElse(zero)

    def updateValue(v: Range[T], r: PositionedResult) = r.updateObject(mkPgObject(v))

    override def valueToSQLLiteral(v: Range[T]) = v.toString

    ///
    private def mkPgObject(v: Range[T]) = {
      val obj = new PGobject
      obj.setType(rangeType)
      obj.setValue(valueToSQLLiteral(v))
      obj
    }
  }
}
