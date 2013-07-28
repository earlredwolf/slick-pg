package com.github.tminglei.slickpg

import java.util.UUID
import scala.slick.driver.PostgresDriver
import scala.slick.lifted._
import FunctionSymbolExtensionMethods._
import scala.slick.ast.Library.{SqlOperator, SqlFunction}
import scala.slick.ast.Node
import scala.reflect.ClassTag
import org.postgresql.util.PGobject
import scala.slick.jdbc.{PositionedResult, PositionedParameters, JdbcType}

trait PgArraySupport { driver: PostgresDriver =>
  import driver.profile.simple._

  trait ArrayImplicits {
    /** for type/name, @see [[org.postgresql.core.Oid]] and [[org.postgresql.jdbc2.TypeInfoCache]]*/
    implicit val uuidListJdbcType = new ArrayListJdbcType[UUID]("uuid")
    implicit val strListJdbcType = new ArrayListJdbcType[String]("text")
    implicit val longListJdbcType = new ArrayListJdbcType[Long]("int8")
    implicit val intListJdbcType = new ArrayListJdbcType[Int]("int4")
    implicit val floatListJdbcType = new ArrayListJdbcType[Float]("float8")
    implicit val boolListJdbcType = new ArrayListJdbcType[Boolean]("bool")
    implicit val dateListJdbcType = new ArrayListJdbcType[java.sql.Date]("date")
    implicit val timeListJdbcType = new ArrayListJdbcType[java.sql.Time]("time")
    implicit val timestampListJdbcType = new ArrayListJdbcType[java.sql.Timestamp]("timestamp")

    ///
    implicit def arrayColumnExtensionMethods[B1](c: Column[List[B1]])(
      implicit tm: JdbcType[B1], tm1: ArrayListJdbcType[B1]) = {
        new ArrayListColumnExtensionMethods[B1, List[B1]](c)
    	}
    implicit def arrayOptionColumnExtensionMethods[B1](c: Column[Option[List[B1]]])(
      implicit tm: JdbcType[B1], tm1: ArrayListJdbcType[B1]) = {
        new ArrayListColumnExtensionMethods[B1, Option[List[B1]]](c)
    	}
  }

  ///////////////////////////////////////////////////////////////////////////////////////

  object ArrayLibrary {
    val Any = new SqlFunction("any")
    val All = new SqlFunction("all")
    val Concatenate = new SqlOperator("||")
    val Contains  = new SqlOperator("@>")
    val ContainedBy = new SqlOperator("<@")
    val Overlap = new SqlOperator("&&")

    val Length = new SqlFunction("array_length")
    val Unnest = new SqlFunction("unnest")
  }

  /** Extension methods for array Columns */
  class ArrayListColumnExtensionMethods[B0, P1](val c: Column[P1])(
          implicit tm0: JdbcType[B0], tm: JdbcType[List[B0]]) extends ExtensionMethods[List[B0], P1] {
    /** required syntax: expression operator ANY (array expression) */
    def any[R](implicit om: o#to[B0, R]) = om.column(ArrayLibrary.Any, n)
    /** required syntax: expression operator ALL (array expression) */
    def all[R](implicit om: o#to[B0, R]) = om.column(ArrayLibrary.All, n)

    def @>[P2, R](e: Column[P2])(implicit om: o#arg[List[B0], P2]#to[Boolean, R]) = {
    		om.column(ArrayLibrary.Contains, n, Node(e))
    	}
    def <@:[P2, R](e: Column[P2])(implicit om: o#arg[List[B0], P2]#to[Boolean, R]) = {
        om.column(ArrayLibrary.ContainedBy, Node(e), n)
      }
    def @&[P2, R](e: Column[P2])(implicit om: o#arg[List[B0], P2]#to[Boolean, R]) = {
        om.column(ArrayLibrary.Overlap, n, Node(e))
      }

    def ++[P2, R](e: Column[P2])(implicit om: o#arg[List[B0], P2]#to[List[B0], R]) = {
        om.column(ArrayLibrary.Concatenate, n, Node(e))
      }
    def + [P2, R](e: Column[P2])(implicit om: o#arg[B0, P2]#to[List[B0], R]) = {
        om.column(ArrayLibrary.Concatenate, n, Node(e))
      }
    def +:[P2, R](e: Column[P2])(implicit om: o#arg[B0, P2]#to[List[B0], R]) = {
        om.column(ArrayLibrary.Concatenate, Node(e), n)
      }
    def length(dim: Column[Int] = ConstColumn(1)) = ArrayLibrary.Length.column[Int](n, Node(dim))
    def unnest[R](implicit om: o#to[B0, R]) = om.column(ArrayLibrary.Unnest, n)
  }

  ///////////////////////////////////////////////////////////////////////////////////////

  class ArrayListJdbcType[T: ClassTag](baseType: String) extends DriverJdbcType[List[T]] {

    def zero: List[T] = Nil

    def sqlType: Int = java.sql.Types.ARRAY

    override def sqlTypeName: String = s"$baseType ARRAY"

    def setValue(v: List[T], p: PositionedParameters) = p.setObject(mkSqlArray(v, p), sqlType)

    def setOption(v: Option[List[T]], p: PositionedParameters) = p.setObjectOption(v.map(mkSqlArray(_, p)), sqlType)

    def nextValue(r: PositionedResult): List[T] = {
      r.nextObjectOption().map(_.asInstanceOf[java.sql.Array])
        .map(_.getArray.asInstanceOf[Array[Any]].map(_.asInstanceOf[T]).toList)
        .getOrElse(zero)
    }

    def updateValue(v: List[T], r: PositionedResult) = r.updateObject(mkPgObject(v))

    override def valueToSQLLiteral(v: List[T]) = buildStr(v).toString

    //--
    private def mkSqlArray(v: List[T], p: PositionedParameters) = {
      val arr = v.map(_.asInstanceOf[AnyRef]).toArray
      p.ps.getConnection.createArrayOf(baseType, arr)
    }

    private def mkPgObject(v: List[T]) = {
      val obj = new PGobject
      obj.setType(sqlTypeName)
      obj.setValue(valueToSQLLiteral(v))
      obj
    }

    /** copy from [[org.postgresql.jdbc4.AbstractJdbc4Connection#createArrayOf(..)]]
      * and [[org.postgresql.jdbc2.AbstractJdbc2Array#escapeArrayElement(..)]] */
    private def buildStr(elements: Seq[Any]): StringBuilder = {
      def escape(s: String) = {
        StringBuilder.newBuilder + '"' appendAll (
          s map {
            c => if (c == '"' || c == '\\') '\\' else c
          }) + '"'
      }

      StringBuilder.newBuilder + '{' append (
        elements map {
          case arr: Seq[Any] => buildStr(arr)
          case o: Any if (o != null) => escape(o.toString)
          case _ => "NULL"
        } mkString(",")) + '}'
    }
  }
}
