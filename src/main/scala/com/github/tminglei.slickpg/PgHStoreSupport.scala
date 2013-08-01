package com.github.tminglei.slickpg

import scala.slick.driver.PostgresDriver
import scala.slick.lifted._
import FunctionSymbolExtensionMethods._
import scala.slick.ast.Library.{SqlFunction, SqlOperator}
import scala.slick.ast.{Library, Node}
import scala.collection.convert.{WrapAsJava, WrapAsScala}
import org.postgresql.util.{HStoreConverter, PGobject}
import scala.slick.jdbc.{PositionedResult, PositionedParameters, JdbcType}

trait PgHStoreSupport extends ImplicitJdbcTypes { driver: PostgresDriver =>

  trait HStoreImplicits {
    implicit val hstoreMapTypeMapper = new HStoreMapJdbcType

    implicit def hstoreMapColumnExtensionMethods(c: Column[Map[String, String]])(
      implicit tm: JdbcType[Map[String, String]], tm1: JdbcType[List[String]]) = {
    		new HStoreColumnExtensionMethods[Map[String, String]](c)
    	}
    implicit def hstoreMapOptionColumnExtensionMethods(c: Column[Option[Map[String,String]]])(
      implicit tm: JdbcType[Map[String, String]], tm1: JdbcType[List[String]]) = {
    		new HStoreColumnExtensionMethods[Option[Map[String, String]]](c)
    	}
  }

  ///////////////////////////////////////////////////////////////////////////////////////

  object HStoreLibrary {
    val On = new SqlOperator("->")
    val Exist   = new SqlFunction("exist")
//    val ExistAll = new SqlOperator("?&")  //can't support, because there exists '?' conflict
//    val ExistAny = new SqlOperator("?|")  //can't support, because there exists '?' conflict
    val Defined = new SqlFunction("defined")
    val Contains = new SqlOperator("@>")
    val ContainedBy = new SqlOperator("<@")
    val AKeys = new SqlFunction("akeys")

    val Concatenate = new SqlOperator("||")
    val Delete = new SqlOperator("-")
  }

  /** Extension methods for hstore Columns */
  class HStoreColumnExtensionMethods[P1](val c: Column[P1])(
            implicit tm: JdbcType[Map[String, String]], tm1: JdbcType[List[String]])
                extends ExtensionMethods[Map[String, String], P1] {

    def +>[P2, R](k: Column[P2])(implicit om: o#arg[String, P2]#to[String, R]) = {
        om.column(HStoreLibrary.On, n, Node(k))
      }
    def >>[T: JdbcType](k: Column[String]) = {
        Library.Cast.column[T](HStoreLibrary.On.typed[String](n, Node(k)))
      }
    def ??[P2, R](k: Column[P2])(implicit om: o#arg[String, P2]#to[Boolean, R]) = {
        om.column(HStoreLibrary.Exist, n, Node(k))
      }
    def ?&[P2, R](k: Column[P2])(implicit om: o#arg[String, P2]#to[Boolean, R]) = {
        om.column(HStoreLibrary.Defined, n, Node(k))
      }
    def @>[P2, R](c2: Column[P2])(implicit om: o#arg[Map[String, String], P2]#to[Boolean, R]) = {
        om.column(HStoreLibrary.Contains, n, Node(c2))
      }
    def <@:[P2, R](c2: Column[P2])(implicit om: o#arg[Map[String, String], P2]#to[Boolean, R]) = {
        om.column(HStoreLibrary.ContainedBy, Node(c2), n)
      }
    def keys[R](implicit om: o#to[List[String], R]) = om.column(HStoreLibrary.AKeys, n)

    def ++[P2, R](c2: Column[P2])(implicit om: o#arg[Map[String, String], P2]#to[Map[String, String], R]) = {
        om.column(HStoreLibrary.Concatenate, n, Node(c2))
      }
    def - [P2, R](c2: Column[P2])(implicit om: o#arg[String, P2]#to[Map[String, String], R]) = {
        om.column(HStoreLibrary.Delete, n, Node(c2))
      }
    def --[P2, R](c2: Column[P2])(implicit om: o#arg[List[String], P2]#to[Map[String, String], R]) = {
        om.column(HStoreLibrary.Delete, n, Node(c2))
      }
    def -/[P2, R](c2: Column[P2])(implicit om: o#arg[Map[String, String], P2]#to[Map[String, String], R]) = {
        om.column(HStoreLibrary.Delete, n, Node(c2))
      }
  }

  ///////////////////////////////////////////////////////////////////////////////////////

  class HStoreMapJdbcType extends DriverJdbcType[Map[String, String]] {

    def zero = Map.empty[String, String]

    def sqlType = java.sql.Types.OTHER

    override def sqlTypeName = "hstore"

    def setValue(v: Map[String, String], p: PositionedParameters) = p.setObject(mkPgObject(v), sqlType)

    def setOption(v: Option[Map[String, String]], p: PositionedParameters) = p.setObjectOption(v.map(mkPgObject), sqlType)

    def nextValue(r: PositionedResult) = {
      r.nextObjectOption().map(_.asInstanceOf[java.util.Map[String, String]])
        .map(WrapAsScala.mapAsScalaMap(_).toMap)
        .getOrElse(zero)
    }

    def updateValue(v: Map[String, String], r: PositionedResult) = r.updateObject(WrapAsJava.mapAsJavaMap(v))

    override def valueToSQLLiteral(v: Map[String, String]) = HStoreConverter.toString(WrapAsJava.mapAsJavaMap(v))

    ///
    private def mkPgObject(v: Map[String, String]) = {
      val obj = new PGobject
      obj.setType(sqlTypeName)
      obj.setValue(valueToSQLLiteral(v))
      obj
    }
  }
}
