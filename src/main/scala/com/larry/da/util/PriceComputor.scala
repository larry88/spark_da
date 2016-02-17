package com.larry.da.util

/**
 * Created by larry on 3/23/15.
 */

object PriceComputor{
  /** *********** compute internalCost  **************************/
  def calculateInternalCost(winprice:Long, markup:Int)={
    try{
      val markupRatio = BigDecimal(markup)./(BigDecimal(100)).doubleValue();
      val internalcost = Math.round(winprice * ( 1 + markupRatio));
      internalcost;
    }catch{
      case _:Exception => 0L;
    }
  }

  /** *********** compute custom cost,and internalCost  **************************
    * cost = if( internalcost > unitprice ) unitprice else unitprice
    * **************************************************/
  def calculateCost(winprice:Long, baseprice:Long, markup:Int, imp_factor:Int)={
    try{
      //double markupRatio = new BigDecimal(markup).divide(new BigDecimal(100)).doubleValue();
      val internalcost = calculateInternalCost(winprice,markup);
      val unitprice = (baseprice*imp_factor) / 1000 ; //convert cpm to unit price

      val cost = if( internalcost > unitprice ) unitprice else internalcost
      (internalcost,cost)
    }catch{
      case _:Exception => (0L,0L);
    }
  }
}



