package com.github.tminglei.slickpg

import scala.slick.driver.PostgresDriver
import scala.slick.lifted.{TypeMapper, Column}

trait PgHStoreSupport extends hstore.PgHStoreExtensions { driver: PostgresDriver =>

  trait HStoreImplicits {
    implicit val hstoreTypeMapper = new hstore.HStoreTypeMapper

    implicit def hstoreColumnExtensionMethods(c: Column[Map[String, String]])(
      implicit tm: TypeMapper[Map[String, String]], tm1: TypeMapper[List[String]]) = {
        new HStoreColumnExtensionMethods[Map[String, String]](c)
      }
    implicit def hstoreOptionColumnExtensionMethods(c: Column[Option[Map[String,String]]])(
      implicit tm: TypeMapper[Map[String, String]], tm1: TypeMapper[List[String]]) = {
        new HStoreColumnExtensionMethods[Option[Map[String, String]]](c)
      }
  }
}
