from tableschema_spss import mappers


def test_bucket_to_filename():
    assert mappers.bucket_to_filename('bucket') == 'bucket.sav'
    assert mappers.bucket_to_filename('bucket.sav') == 'bucket.sav'
    assert mappers.bucket_to_filename('bucket.zsav') == 'bucket.zsav'
