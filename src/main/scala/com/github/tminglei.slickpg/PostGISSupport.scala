package com.github.tminglei.slickpg

import com.vividsolutions.jts.io._
import com.vividsolutions.jts.geom._
import java.sql.SQLException
import scala.slick.driver.PostgresDriver
import scala.slick.lifted._
import scala.slick.ast.Library.{SqlFunction, SqlOperator}
import scala.slick.ast.{LiteralNode, Node}
import scala.slick.jdbc.{PositionedResult, PositionedParameters}

trait PostGISSupport { driver: PostgresDriver =>
  import driver.profile.simple._

  private trait GeometryTypesImplicits {
    implicit val geometryJdbcType = new GeometryJdbcType[Geometry]
    implicit val pointJdbcType = new GeometryJdbcType[Point]
    implicit val polygonJdbcType = new GeometryJdbcType[Polygon]
    implicit val lineStringJdbcType = new GeometryJdbcType[LineString]
    implicit val linearRingJdbcType = new GeometryJdbcType[LinearRing]
    implicit val geometryCollectionJdbcType = new GeometryJdbcType[GeometryCollection]
    implicit val multiPointJdbcType = new GeometryJdbcType[MultiPoint]
    implicit val multiPolygonJdbcType = new GeometryJdbcType[MultiPolygon]
    implicit val multiLineStringJdbcType = new GeometryJdbcType[MultiLineString]
  }

  trait PostGISImplicits extends GeometryTypesImplicits {
    implicit def geometryColumnExtensionMethods[G1 <: Geometry](c: Column[G1])(
      implicit tm: GeometryJdbcType[G1]) = {
    		new GeometryColumnExtensionMethods[G1](c)
    	}
    implicit def geometryOptionColumnExtensionMethods[G1 <: Geometry](c: Column[Option[G1]])(
      implicit tm: GeometryJdbcType[G1]) = {
    		new GeometryColumnExtensionMethods[Option[G1]](c)
    	}
  }

  trait PostGISAssistants extends GeometryTypesImplicits {
    /** Geometry Constructors */
    def geomFromText[P, R](wkt: Column[P], srid: Option[Int] = None)(
      implicit om: OptionMapperDSL.arg[String, P]#to[Geometry, R]) = srid match {
        case Some(srid) => om.column(PostGISLibrary.GeomFromText, Node(wkt), LiteralNode(srid))
        case None   => om.column(PostGISLibrary.GeomFromText, Node(wkt))
      }
    def geomFromWKB[P, R](wkb: Column[P], srid: Option[Int] = None)(
      implicit om: OptionMapperDSL.arg[Array[Byte], P]#to[Geometry, R]) = srid match {
        case Some(srid) => om.column(PostGISLibrary.GeomFromWKB, Node(wkb), LiteralNode(srid))
        case None   => om.column(PostGISLibrary.GeomFromWKB, Node(wkb))
      }
    def geomFromEWKT[P, R](ewkt: Column[P])(
      implicit om: OptionMapperDSL.arg[String, P]#to[Geometry, R]) = {
        om.column(PostGISLibrary.GeomFromEWKT, Node(ewkt))
      }
    def geomFromEWKB[P, R](ewkb: Column[P])(
      implicit om: OptionMapperDSL.arg[Array[Byte], P]#to[Geometry, R]) = {
        om.column(PostGISLibrary.GeomFromEWKB, Node(ewkb))
      }
    def geomFromGML[P, R](gml: Column[P], srid: Option[Int] = None)(
      implicit om: OptionMapperDSL.arg[String, P]#to[Geometry, R]) = srid match {
        case Some(srid) => om.column(PostGISLibrary.GeomFromGML, Node(gml), LiteralNode(srid))
        case None   => om.column(PostGISLibrary.GeomFromGML, Node(gml))
      }
    def geomFromKML[P, R](kml: Column[P])(
      implicit om: OptionMapperDSL.arg[String, P]#to[Geometry, R]) = {
        om.column(PostGISLibrary.GeomFromKML, Node(kml))
      }
    def geomFromGeoJSON[P, R](json: Column[P])(
      implicit om: OptionMapperDSL.arg[String, P]#to[Geometry, R]) = {
        om.column(PostGISLibrary.GeomFromGeoJSON, Node(json))
      }
    def makeBox[P1, P2, R](lowLeftPoint: Column[P1], upRightPoint: Column[P2])(
      implicit om: OptionMapperDSL.arg[Geometry, P1]#arg[Geometry, P2]#to[Geometry, R]) = {
        om.column(PostGISLibrary.MakeBox, Node(lowLeftPoint), Node(upRightPoint))
      }
    def makePoint[P1, P2, R](x: Column[P1], y: Column[P2], z: Option[Double] = None, m: Option[Double] = None)(
      implicit om: OptionMapperDSL.arg[Double, P1]#arg[Double, P2]#to[Geometry, R]) = (z, m) match {
        case (Some(z), Some(m)) => om.column(PostGISLibrary.MakePoint, Node(x), Node(y), LiteralNode(z), LiteralNode(m))
        case (Some(z), None) => om.column(PostGISLibrary.MakePoint, Node(x), Node(y), LiteralNode(z))
        case (None, Some(m)) => om.column(PostGISLibrary.MakePointM, Node(x), Node(y), Node(m))
        case (None, None)    => om.column(PostGISLibrary.MakePoint, Node(x), Node(y))
      }
  }

  //////////////////////////////////////////////////////////////////////////////////

  object PostGISLibrary {
    /** Geometry Operators */
    val BoxIntersects = new SqlOperator("&&")
    val BoxIntersects3D = new SqlOperator("&&&")
    val BoxContains = new SqlOperator("~")
    val BoxContainedBy = new SqlOperator("@")
//    val BoxEquals = new SqlOperator("=")  // it's not necessary
    val PointDistance = new SqlOperator("<->")
    val BoxDistance = new SqlOperator("<#>")

    val BoxLooseLeft = new SqlOperator("&<")
    val BoxStrictLeft = new SqlOperator("<<")
    val BoxLooseBelow = new SqlOperator("&<|")
    val BoxStrictBelow = new SqlOperator("<<|")
    val BoxLooseRight = new SqlOperator("&>")
    val BoxStrictRight = new SqlOperator(">>")
    val BoxLooseAbove = new SqlOperator("|&>")
    val BoxStrictAbove = new SqlOperator("|>>")

    /** Geometry Constructors */
    val GeomFromText = new SqlFunction("ST_GeomFromText")
    val GeomFromWKB = new SqlFunction("ST_GeomFromWKB")
    val GeomFromEWKT = new SqlFunction("ST_GeomFromEWKT")
    val GeomFromEWKB = new SqlFunction("ST_GeomFromEWKB")
    val GeomFromGML = new SqlFunction("ST_GeomFromGML")
    val GeomFromKML = new SqlFunction("ST_GeomFromKML")
    val GeomFromGeoJSON = new SqlFunction("ST_GeomFromGeoJSON")
    val MakeBox = new SqlFunction("ST_MakeBox2D")
    val MakePoint = new SqlFunction("ST_MakePoint")
    val MakePointM = new SqlFunction("ST_MakePointM")

    /** Geometry Accessors */
    val GeometryType = new SqlFunction("ST_GeometryType")
    val SRID = new SqlFunction("ST_SRID")
    val IsValid = new SqlFunction("ST_IsValid")
    val IsClosed = new SqlFunction("ST_IsClosed")
    val IsCollection = new SqlFunction("ST_IsCollection")
    val IsEmpty = new SqlFunction("ST_IsEmpty")
    val IsRing = new SqlFunction("ST_IsRing")
    val IsSimple = new SqlFunction("ST_IsSimple")
    val Area = new SqlFunction("ST_Area")
    val Boundary = new SqlFunction("ST_Boundary")
    val Dimension = new SqlFunction("ST_Dimension")
    val CoordDim = new SqlFunction("ST_CoordDim")
    val NDims = new SqlFunction("ST_NDims")
    val NPoints = new SqlFunction("ST_NPoints")
    val NRings = new SqlFunction("ST_NRings")

    /** Geometry Outputs */
    val AsBinary = new SqlFunction("ST_AsBinary")
    val AsText = new SqlFunction("ST_AsText")
    val AsLatLonText = new SqlFunction("ST_AsLatLonText")
    val AsEWKB = new SqlFunction("ST_AsEWKB")
    val AsEWKT = new SqlFunction("ST_AsEWKT")
    val AsHEXEWKB = new SqlFunction("ST_AsHEXEWKB")
    val AsGeoJSON = new SqlFunction("ST_AsGeoJSON")
    val AsGeoHash = new SqlFunction("ST_GeoHash")
    val AsGML = new SqlFunction("ST_AsGML")
    val AsKML = new SqlFunction("ST_AsKML")
    val AsSVG = new SqlFunction("ST_AsSVG")
    val AsX3D = new SqlFunction("ST_AsX3D")

    /** Spatial Relationships */
    val HasArc = new SqlFunction("ST_HasArc")
    val Equals = new SqlFunction("ST_Equals")
    val OrderingEquals = new SqlFunction("ST_OrderingEquals")
    val Overlaps = new SqlFunction("ST_Overlaps")
    val Intersects = new SqlFunction("ST_Intersects")
    val Crosses = new SqlFunction("ST_Crosses")
    val Disjoint = new SqlFunction("ST_Disjoint")
    val Contains = new SqlFunction("ST_Contains")
    val ContainsProperly = new SqlFunction("ST_ContainsProperly")
    val Within = new SqlFunction("ST_Within")
    val DWithin = new SqlFunction("ST_DWithin")
    val DFullyWithin = new SqlFunction("ST_DFullyWithin")
    val Touches = new SqlFunction("ST_Touches")
    val Relate = new SqlFunction("ST_Relate")

    /** Spatial Measurements */
    val Azimuth = new SqlFunction("ST_Azimuth")
    val Centroid = new SqlFunction("ST_Centroid")
    val ClosestPoint = new SqlFunction("ST_ClosestPoint")
    val PointOnSurface = new SqlFunction("ST_PointOnSurface")
    val Project = new SqlFunction("ST_Project")
    val Length = new SqlFunction("ST_Length")
    val Length3D = new SqlFunction("ST_3DLength")
    val Perimeter = new SqlFunction("ST_Perimeter")
    val Distance = new SqlFunction("ST_Distance")
    val DistanceSphere = new SqlFunction("ST_Distance_Sphere")
    val MaxDistance = new SqlFunction("ST_MaxDistance")
    val HausdorffDistance = new SqlFunction("ST_HausdorffDistance")
    val LongestLine = new SqlFunction("ST_LongestLine")
    val ShortestLine = new SqlFunction("ST_ShortestLine")

    /** Geometry Processing */
    val SetSRID = new SqlFunction("ST_SetSRID")
    val Transform = new SqlFunction("ST_Transform")
    val Simplify = new SqlFunction("ST_Simplify")
    val RemoveRepeatedPoints = new SqlFunction("ST_RemoveRepeatedPoints")
    val SimplifyPreserveTopology = new SqlFunction("ST_SimplifyPreserveTopology")
    val Difference = new SqlFunction("ST_Difference")
    val SymDifference = new SqlFunction("ST_SymDifference")
    val Intersection = new SqlFunction("ST_Intersection")
    val SharedPaths = new SqlFunction("ST_SharedPaths")
    val Split = new SqlFunction("ST_Split")
    val MinBoundingCircle = new SqlFunction("ST_MinimumBoundingCircle")

    val Buffer = new SqlFunction("ST_Buffer")
    val Multi = new SqlFunction("ST_Multi")
    val LineMerge = new SqlFunction("ST_LineMerge")
    val CollectionExtract = new SqlFunction("ST_CollectionExtract")
    val CollectionHomogenize = new SqlFunction("ST_CollectionHomogenize")
    val AddPoint = new SqlFunction("ST_AddPoint")
    val SetPoint = new SqlFunction("ST_SetPoint")
    val RemovePoint = new SqlFunction("ST_RemovePoint")
    val Reverse = new SqlFunction("ST_Reverse")
    val Scale = new SqlFunction("ST_Scale")
    val Segmentize = new SqlFunction("ST_Segmentize")
    val Snap = new SqlFunction("ST_Snap")
    val Translate = new SqlFunction("ST_Translate")
  }

  /** Extension methods for hstore Columns */
  class GeometryColumnExtensionMethods[P1](val c: Column[P1]) extends ExtensionMethods[Geometry, P1] with GeometryTypesImplicits {
    /** Geometry Operators */
    def @&&[P2, R](geom: Column[P2])(implicit om: o#arg[Geometry, P2]#to[Boolean, R]) = {
    		om.column(PostGISLibrary.BoxIntersects, n, Node(geom))
    	}
    def @&&&[P2, R](geom: Column[P2])(implicit om: o#arg[Geometry, P2]#to[Boolean, R]) = {
    		om.column(PostGISLibrary.BoxIntersects3D, n, Node(geom))
    	}
    def @>[P2, R](geom: Column[P2])(implicit om: o#arg[Geometry, P2]#to[Boolean, R]) = {
    		om.column(PostGISLibrary.BoxContains, n, Node(geom))
    	}
    def <@[P2, R](geom: Column[P2])(implicit om: o#arg[Geometry, P2]#to[Boolean, R]) = {
    		om.column(PostGISLibrary.BoxContainedBy, n, Node(geom))
    	}
    def <->[P2, R](geom: Column[P2])(implicit om: o#arg[Geometry, P2]#to[Double, R]) = {
    		om.column(PostGISLibrary.PointDistance, n, Node(geom))
    	}
    def <#>[P2, R](geom: Column[P2])(implicit om: o#arg[Geometry, P2]#to[Double, R]) = {
    		om.column(PostGISLibrary.BoxDistance, n, Node(geom))
    	}

    def &<[P2, R](geom: Column[P2])(implicit om: o#arg[Geometry, P2]#to[Boolean, R]) = {
    		om.column(PostGISLibrary.BoxLooseLeft, n, Node(geom))
    	}
    def <<[P2, R](geom: Column[P2])(implicit om: o#arg[Geometry, P2]#to[Boolean, R]) = {
        om.column(PostGISLibrary.BoxStrictLeft, n, Node(geom))
      }
    def &<|[P2, R](geom: Column[P2])(implicit om: o#arg[Geometry, P2]#to[Boolean, R]) = {
        om.column(PostGISLibrary.BoxLooseBelow, n, Node(geom))
      }
    def <<|[P2, R](geom: Column[P2])(implicit om: o#arg[Geometry, P2]#to[Boolean, R]) = {
        om.column(PostGISLibrary.BoxStrictBelow, n, Node(geom))
      }
    def &>[P2, R](geom: Column[P2])(implicit om: o#arg[Geometry, P2]#to[Boolean, R]) = {
        om.column(PostGISLibrary.BoxLooseRight, n, Node(geom))
      }
    def >>[P2, R](geom: Column[P2])(implicit om: o#arg[Geometry, P2]#to[Boolean, R]) = {
        om.column(PostGISLibrary.BoxStrictRight, n, Node(geom))
      }
    def |&>[P2, R](geom: Column[P2])(implicit om: o#arg[Geometry, P2]#to[Boolean, R]) = {
        om.column(PostGISLibrary.BoxLooseAbove, n, Node(geom))
      }
    def |>>[P2, R](geom: Column[P2])(implicit om: o#arg[Geometry, P2]#to[Boolean, R]) = {
        om.column(PostGISLibrary.BoxStrictAbove, n, Node(geom))
      }

    /** Geometry Accessors */
    def geomType[R](implicit om: o#to[String, R]) = {
        om.column(PostGISLibrary.GeometryType, n)
      }
    def srid[R](implicit om: o#to[Int, R]) = {
        om.column(PostGISLibrary.SRID, n)
      }
    def isValid[R](implicit om: o#to[Boolean, R]) = {
        om.column(PostGISLibrary.IsValid, n)
      }
    def isClosed[R](implicit om: o#to[Boolean, R]) = {
        om.column(PostGISLibrary.IsClosed, n)
      }
    def isCollection[R](implicit om: o#to[Boolean, R]) = {
        om.column(PostGISLibrary.IsCollection, n)
      }
    def isEmpty[R](implicit om: o#to[Boolean, R]) = {
        om.column(PostGISLibrary.IsEmpty, n)
      }
    def isRing[R](implicit om: o#to[Boolean, R]) = {
        om.column(PostGISLibrary.IsRing, n)
      }
    def isSimple[R](implicit om: o#to[Boolean, R]) = {
        om.column(PostGISLibrary.IsSimple, n)
      }
    def hasArc[R](implicit om: o#to[Boolean, R]) = {
        om.column(PostGISLibrary.HasArc, n)
      }
    def area[R](implicit om: o#to[Float, R]) = {
        om.column(PostGISLibrary.Area, n)
      }
    def boundary[R](implicit om: o#to[Geometry, R]) = {
        om.column(PostGISLibrary.Boundary, n)
      }
    def dimension[R](implicit om: o#to[Int, R]) = {
        om.column(PostGISLibrary.Dimension, n)
      }
    def coordDim[R](implicit om: o#to[Int, R]) = {
        om.column(PostGISLibrary.CoordDim, n)
      }
    def nDims[R](implicit om: o#to[Int, R]) = {
        om.column(PostGISLibrary.NDims, n)
      }
    def nPoints[R](implicit om: o#to[Int, R]) = {
        om.column(PostGISLibrary.NPoints, n)
      }
    def nRings[R](implicit om: o#to[Int, R]) = {
        om.column(PostGISLibrary.NRings, n)
      }

    /** Geometry Outputs */
    def asBinary[R](NDRorXDR: Option[String] = None)(implicit om: o#to[Array[Byte], R]) = NDRorXDR match {
        case Some(endian) => om.column(PostGISLibrary.AsBinary, n, LiteralNode(endian))
        case None   => om.column(PostGISLibrary.AsBinary, n)
      }
    def asText[R](implicit om: o#to[String, R]) = {
        om.column(PostGISLibrary.AsText, n)
      }
    def asLatLonText[R](format: Option[String] = None)(implicit om: o#to[String, R]) = format match {
        case Some(fmt) => om.column(PostGISLibrary.AsLatLonText, n, LiteralNode(fmt))
        case None   => om.column(PostGISLibrary.AsLatLonText, n)
      }
    def asEWKB[R](NDRorXDR: Option[String] = None)(implicit om: o#to[Array[Byte], R]) = NDRorXDR match {
        case Some(endian) => om.column(PostGISLibrary.AsEWKB, n, LiteralNode(endian))
        case None   => om.column(PostGISLibrary.AsEWKB, n)
      }
    def asEWKT[R](implicit om: o#to[String, R]) = {
        om.column(PostGISLibrary.AsEWKT, n)
      }
    def asHEXEWKB[R](NDRorXDR: Option[String] = None)(implicit om: o#to[String, R]) = NDRorXDR match {
        case Some(endian) => om.column(PostGISLibrary.AsHEXEWKB, n, LiteralNode(endian))
        case None   => om.column(PostGISLibrary.AsHEXEWKB, n)
      }
    def asGeoJSON[R](maxDigits: Column[Int] = ConstColumn(15), options: Column[Int] = ConstColumn(0),
      geoJsonVer: Option[Int] = None)(implicit om: o#to[String, R]) = geoJsonVer match {
        case Some(ver) => om.column(PostGISLibrary.AsGeoJSON, LiteralNode(ver), n, Node(maxDigits), Node(options))
        case None   => om.column(PostGISLibrary.AsGeoJSON, n, Node(maxDigits), Node(options))
      }
    def asGeoHash[R](maxChars: Option[Int] = None)(implicit om: o#to[String, R]) = maxChars match {
        case Some(charNum) => om.column(PostGISLibrary.AsHEXEWKB, n, LiteralNode(charNum))
        case None   => om.column(PostGISLibrary.AsHEXEWKB, n)
      }
    def asGML[R](maxDigits: Column[Int] = ConstColumn(15), options: Column[Int] = ConstColumn(0),
      version: Option[Int] = None,  nPrefix: Option[String] = None)(implicit om: o#to[String, R]) = (version, nPrefix) match {
        case (Some(ver), Some(prefix)) => om.column(PostGISLibrary.AsGML, LiteralNode(ver), n, Node(maxDigits), Node(options), LiteralNode(prefix))
        case (Some(ver), None) => om.column(PostGISLibrary.AsGML, LiteralNode(ver), n, Node(maxDigits), Node(options))
        case (_, _)   => om.column(PostGISLibrary.AsGML, n, Node(maxDigits), Node(options))
      }
    def asKML[R](maxDigits: Column[Int] = ConstColumn(15), version: Option[Int] = None,  nPrefix: Option[String] = None)(
      implicit om: o#to[String, R]) = (version, nPrefix) match {
        case (Some(ver), Some(prefix)) => om.column(PostGISLibrary.AsKML, LiteralNode(ver), n, Node(maxDigits), LiteralNode(prefix))
        case (Some(ver), None) => om.column(PostGISLibrary.AsKML, LiteralNode(ver), n, Node(maxDigits))
        case (_, _)   => om.column(PostGISLibrary.AsKML, n, Node(maxDigits))
      }
    def asSVG[R](rel: Column[Int] = ConstColumn(0), maxDigits: Column[Int] = ConstColumn(15))(
      implicit om: o#to[String, R]) = {
        om.column(PostGISLibrary.AsSVG, n, Node(rel), Node(maxDigits))
      }
    def asX3D[R](maxDigits: Column[Int] = ConstColumn(15), options: Column[Int] = ConstColumn(0))(
      implicit om: o#to[String, R]) = {
        om.column(PostGISLibrary.AsX3D, n, Node(maxDigits), Node(options))
      }

    /** Spatial Relationships */
    def gEquals[P2, R](geom: Column[P2])(implicit om: o#arg[Geometry, P2]#to[Boolean, R]) = {
        om.column(PostGISLibrary.Equals, n, Node(geom))
      }
    def orderingEquals[P2, R](geom: Column[P2])(implicit om: o#arg[Geometry, P2]#to[Boolean, R]) = {
        om.column(PostGISLibrary.OrderingEquals, n, Node(geom))
      }
    def overlaps[P2, R](geom: Column[P2])(implicit om: o#arg[Geometry, P2]#to[Boolean, R]) = {
        om.column(PostGISLibrary.Overlaps, n, Node(geom))
      }
    def intersects[P2, R](geom: Column[P2])(implicit om: o#arg[Geometry, P2]#to[Boolean, R]) = {
        om.column(PostGISLibrary.Intersects, n, Node(geom))
      }
    def crosses[P2, R](geom: Column[P2])(implicit om: o#arg[Geometry, P2]#to[Boolean, R]) = {
        om.column(PostGISLibrary.Crosses, n, Node(geom))
      }
    def disjoint[P2, R](geom: Column[P2])(implicit om: o#arg[Geometry, P2]#to[Boolean, R]) = {
        om.column(PostGISLibrary.Disjoint, n, Node(geom))
      }
    def contains[P2, R](geom: Column[P2])(implicit om: o#arg[Geometry, P2]#to[Boolean, R]) = {
        om.column(PostGISLibrary.Contains, n, Node(geom))
      }
    def containsProperly[P2, R](geom: Column[P2])(implicit om: o#arg[Geometry, P2]#to[Boolean, R]) = {
        om.column(PostGISLibrary.ContainsProperly, n, Node(geom))
      }
    def within[P2, R](geom: Column[P2])(implicit om: o#arg[Geometry, P2]#to[Boolean, R]) = {
        om.column(PostGISLibrary.Within, n, Node(geom))
      }
    def dWithin[P2, R](geom: Column[P2], distance: Column[Double])(
      implicit om: o#arg[Geometry, P2]#to[Boolean, R]) = {
        om.column(PostGISLibrary.DWithin, n, Node(geom), Node(distance))
      }
    def dFullyWithin[P2, R](geom: Column[P2], distance: Column[Double])(
      implicit om: o#arg[Geometry, P2]#to[Boolean, R]) = {
        om.column(PostGISLibrary.DFullyWithin, n, Node(geom), Node(distance))
      }
    def touches[P2, R](geom: Column[P2])(implicit om: o#arg[Geometry, P2]#to[Boolean, R]) = {
        om.column(PostGISLibrary.Touches, n, Node(geom))
      }
    def relate[P2, R](geom: Column[P2], matrixPattern: Column[String])(
      implicit om: o#arg[Geometry, P2]#to[Boolean, R]) = {
        om.column(PostGISLibrary.Relate, n, Node(geom), Node(matrixPattern))
      }
    def relatePattern[P2, R](geom: Column[P2], boundaryNodeRule: Option[Int] = None)(
      implicit om: o#arg[Geometry, P2]#to[String, R]) = boundaryNodeRule match {
        case Some(rule) => om.column(PostGISLibrary.Relate, n, Node(geom), LiteralNode(rule))
        case None    => om.column(PostGISLibrary.Relate, n, Node(geom))
      }

    /** Spatial Measurements */
    def azimuth[P2, R](geom: Column[P2])(implicit om: o#arg[Geometry, P2]#to[Float, R]) = {
        om.column(PostGISLibrary.Azimuth, n, Node(geom))
      }
    def centroid[R](implicit om: o#to[Point, R]) = {
        om.column(PostGISLibrary.Centroid, n)
      }
    def closestPoint[P2, R](geom: Column[P2])(implicit om: o#arg[Geometry, P2]#to[Point, R]) = {
        om.column(PostGISLibrary.ClosestPoint, n, Node(geom))
      }
    def pointOnSurface[R](implicit om: o#to[Point, R]) = {
        om.column(PostGISLibrary.PointOnSurface, n)
      }
    def project[R](distance: Column[Float], azimuth: Column[Float])(implicit om: o#to[Point, R]) = {
        om.column(PostGISLibrary.Project, n, Node(distance), Node(azimuth))
      }
    def length[R](implicit om: o#to[Float, R]) = {
        om.column(PostGISLibrary.Length, n)
      }
    def length3d[R](implicit om: o#to[Float, R]) = {
        om.column(PostGISLibrary.Length3D, n)
      }
    def perimeter[R](implicit om: o#to[Float, R]) = {
        om.column(PostGISLibrary.Perimeter, n)
      }
    def distance[P2, R](geom: Column[P2])(implicit om: o#arg[Geometry, P2]#to[Float, R]) = {
        om.column(PostGISLibrary.Distance, n, Node(geom))
      }
    def distanceSphere[P2, R](geom: Column[P2])(implicit om: o#arg[Geometry, P2]#to[Float, R]) = {
        om.column(PostGISLibrary.DistanceSphere, n, Node(geom))
      }
    def maxDistance[P2, R](geom: Column[P2])(implicit om: o#arg[Geometry, P2]#to[Float, R]) = {
        om.column(PostGISLibrary.MaxDistance, n, Node(geom))
      }
    def hausdorffDistance[P2, R](geom: Column[P2], densifyFrac: Option[Float] = None)(
      implicit om: o#arg[Geometry, P2]#to[Float, R]) = densifyFrac match {
        case Some(denFrac) => om.column(PostGISLibrary.HausdorffDistance, n, Node(geom), LiteralNode(denFrac))
        case None   => om.column(PostGISLibrary.HausdorffDistance, n, Node(geom))
      }
    def longestLine[P2, R](geom: Column[P2])(implicit om: o#arg[Geometry, P2]#to[LineString, R]) = {
        om.column(PostGISLibrary.LongestLine, n, Node(geom))
      }
    def shortestLine[P2, R](geom: Column[P2])(implicit om: o#arg[Geometry, P2]#to[LineString, R]) = {
        om.column(PostGISLibrary.ShortestLine, n, Node(geom))
      }

    /** Geometry Processing */
    def setSRID[R](srid: Column[Int])(implicit om: o#to[Geometry, R]) = {
        om.column(PostGISLibrary.SetSRID, n, Node(srid))
      }
    def transform[R](srid: Column[Int])(implicit om: o#to[Geometry, R]) = {
        om.column(PostGISLibrary.Transform, n, Node(srid))
      }
    def simplify[R](tolerance: Column[Float])(implicit om: o#to[Geometry, R]) = {
        om.column(PostGISLibrary.Simplify, n, Node(tolerance))
      }
    def removeRepeatedPoints[R](implicit om: o#to[Geometry, R]) = {
        om.column(PostGISLibrary.RemoveRepeatedPoints, n)
      }
    def simplifyPreserveTopology[R](tolerance: Column[Float])(implicit om: o#to[Geometry, R]) = {
        om.column(PostGISLibrary.SimplifyPreserveTopology, n, Node(tolerance))
      }
    def difference[P2, R](geom: Column[P2])(implicit om: o#arg[Geometry, P2]#to[Geometry, R]) = {
        om.column(PostGISLibrary.Difference, n, Node(geom))
      }
    def symDifference[P2, R](geom: Column[P2])(implicit om: o#arg[Geometry, P2]#to[Geometry, R]) = {
        om.column(PostGISLibrary.SymDifference, n, Node(geom))
      }
    def intersection[P2, R](geom: Column[P2])(implicit om: o#arg[Geometry, P2]#to[Geometry, R]) = {
        om.column(PostGISLibrary.Intersection, n, Node(geom))
      }
    def sharedPaths[P2, R](geom: Column[P2])(implicit om: o#arg[Geometry, P2]#to[Geometry, R]) = {
        om.column(PostGISLibrary.SharedPaths, n, Node(geom))
      }
    def split[P2, R](blade: Column[P2])(implicit om: o#arg[Geometry, P2]#to[Geometry, R]) = {
        om.column(PostGISLibrary.Split, n, Node(blade))
      }
    def minBoundingCircle[R](segNumPerQtrCircle: Column[Int] = ConstColumn(48))(implicit om: o#to[Geometry, R]) = {
        om.column(PostGISLibrary.MinBoundingCircle, n, Node(segNumPerQtrCircle))
      }

    def buffer[R](radius: Column[Float], bufferStyles: Option[String] = None)(
      implicit om: o#to[Geometry, R]) = bufferStyles match {
        case Some(styles) => om.column(PostGISLibrary.Buffer, n, Node(radius), Node(styles))
        case None   =>  om.column(PostGISLibrary.Buffer, n, Node(radius))
      }
    def multi[R](implicit om: o#to[Geometry, R]) = {
        om.column(PostGISLibrary.Multi, n)
      }
    def lineMerge[R](implicit om: o#to[Geometry, R]) = {
        om.column(PostGISLibrary.LineMerge, n)
      }
    def collectionExtract[R](tpe: Column[Int])(implicit om: o#to[Geometry, R]) = {
        om.column(PostGISLibrary.CollectionExtract, n, Node(tpe))
      }
    def collectionHomogenize[R](implicit om: o#to[Geometry, R]) = {
        om.column(PostGISLibrary.CollectionHomogenize, n)
      }
    def addPoint[P2, R](point: Column[P2], position: Option[Int] = None)(
      implicit om: o#arg[Geometry, P2]#to[Geometry, R]) = position match {
        case Some(pos) => om.column(PostGISLibrary.AddPoint, n, Node(point), Node(pos))
        case None   =>  om.column(PostGISLibrary.AddPoint, n, Node(point))
      }
    def setPoint[P2, R](point: Column[P2], position: Column[Int])(
      implicit om: o#arg[Geometry, P2]#to[Geometry, R]) = {
        om.column(PostGISLibrary.SetPoint, n, Node(position), Node(point))
      }
    def removePoint[R](offset: Column[Int])(implicit om: o#to[Geometry, R]) = {
        om.column(PostGISLibrary.RemovePoint, n, Node(offset))
      }
    def reverse[R](implicit om: o#to[Geometry, R]) = {
        om.column(PostGISLibrary.Reverse, n)
      }
    def scale[R](xFactor: Column[Float], yFactor: Column[Float], zFactor: Option[Float] = None)(
      implicit om: o#to[Geometry, R]) = zFactor match {
        case Some(zFac) => om.column(PostGISLibrary.Scale, n, Node(xFactor), Node(yFactor), LiteralNode(zFac))
        case None   =>  om.column(PostGISLibrary.Scale, n, Node(xFactor), Node(yFactor))
      }
    def segmentize[R](maxLength: Column[Float])(implicit om: o#to[Geometry, R]) = {
        om.column(PostGISLibrary.Segmentize, n, Node(maxLength))
      }
    def snap[P2, R](reference: Column[P2], tolerance: Column[Float])(
      implicit om: o#arg[Geometry, P2]#to[Geometry, R]) = {
        om.column(PostGISLibrary.Snap, n, Node(reference), Node(tolerance))
      }
    def translate[R](deltaX: Column[Float], deltaY: Column[Float], deltaZ: Option[Float] = None)(
      implicit om: o#to[Geometry, R]) = deltaZ match {
        case Some(deltaZ) => om.column(PostGISLibrary.Translate, n, Node(deltaX), Node(deltaY), LiteralNode(deltaZ))
        case None   =>  om.column(PostGISLibrary.Translate, n, Node(deltaX), Node(deltaY))
      }
  }

  //////////////////////////////////////////////////////////////////////////////////

  class GeometryJdbcType[T <: Geometry] extends DriverJdbcType[T] {
    
    def zero: T = null.asInstanceOf[T]

    def sqlType: Int = java.sql.Types.OTHER

    override def sqlTypeName: String = "geometry"

    def setValue(v: T, p: PositionedParameters) = p.setBytes(toBytes(v))

    def setOption(v: Option[T], p: PositionedParameters) = if (v.isDefined) setValue(v.get, p) else p.setNull(sqlType)

    def nextValue(r: PositionedResult): T = r.nextStringOption().map(fromLiteral).getOrElse(zero)

    def updateValue(v: T, r: PositionedResult) = r.updateBytes(toBytes(v))

    override def valueToSQLLiteral(v: T) = toLiteral(v)

    //////
    private val wktWriterHolder = new ThreadLocal[WKTWriter]
    private val wktReaderHolder = new ThreadLocal[WKTReader]
    private val wkbWriterHolder = new ThreadLocal[WKBWriter]
    private val wkbReaderHolder = new ThreadLocal[WKBReader]

    private def toLiteral(geom: Geometry): String = {
      if (wktWriterHolder.get == null) wktWriterHolder.set(new WKTWriter())
      wktWriterHolder.get.write(geom)
    }
    private def fromLiteral(value: String): T = {
      if (wktReaderHolder.get == null) wktReaderHolder.set(new WKTReader())
      splitRSIDAndWKT(value) match {
        case (srid, wkt) => {
          val geom =
            if (wkt.startsWith("00") || wkt.startsWith("01"))
              fromBytes(WKBReader.hexToBytes(wkt))
            else wktReaderHolder.get.read(wkt)

          if (srid != -1) geom.setSRID(srid)
          geom.asInstanceOf[T]
        }
      }
    }

    private def toBytes(geom: Geometry): Array[Byte] = {
      if (wkbWriterHolder.get == null) wkbWriterHolder.set(new WKBWriter(2, true))
      wkbWriterHolder.get.write(geom)
    }
    private def fromBytes[T](bytes: Array[Byte]): T = {
      if (wkbReaderHolder.get == null) wkbReaderHolder.set(new WKBReader())
      wkbReaderHolder.get.read(bytes).asInstanceOf[T]
    }

    /** copy from [[org.postgis.PGgeometry#splitSRID]] */
    private def splitRSIDAndWKT(value: String): (Int, String) = {
      if (value.startsWith("SRID=")) {
        val index = value.indexOf(';', 5); // srid prefix length is 5
        if (index == -1) {
          throw new SQLException("Error parsing Geometry - SRID not delimited with ';' ");
        } else {
          val srid = Integer.parseInt(value.substring(0, index))
          val wkt = value.substring(index + 1)
          (srid, wkt)
        }
      } else (-1, value)
    }
  }
}
