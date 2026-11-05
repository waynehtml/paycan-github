# Databricks notebook source
class Integration_Test(object):
  
  def count_check():
    assert((qualified_rawDF.count() == qualified_standardizedDF.count()) and (qualified_standardizedDF.count() == qualified_curatedDF.count()))
  
  def null_check():
    assert((len(qualified_standardizedDF.head(1)) > 0) and (len(qualified_curatedDF.head(1)) > 0))
    
  def test_credit_position():
    assert(isinstance(validate_col(qualified_rawDF, file_name, get_info(file_name, config)),DataFrame) == True)
    assert(isinstance(var_uc(qualified_standardizedDF, ["credit_entry_id"], "FLAG_CREDIT_POSITION_ID"),DataFrame) == True)
  
  def test_participant_institution():
    assert(isinstance(validate_col(qualified_rawDF, file_name, get_info(file_name, config)),DataFrame) == True)
    assert(isinstance(var_uc(qualified_standardizedDF, ["institution_number", "participant_number", "effective_ts", "expire_ts"], "FLAG_PART_INST_DUPLICATE"),DataFrame) == True)
    
  def test_payment():
    assert(isinstance(validate_col(qualified_rawDF, file_name, get_info(file_name, config)),DataFrame) == True)
    assert(isinstance(var_uc(qualified_standardizedDF, ["PAYMENT_ID", "STATUS_TYPE_CD"], "FLAG_PYMNT_STATUS"),DataFrame) == True)

# COMMAND ----------

