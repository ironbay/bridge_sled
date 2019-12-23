defmodule Bridge.Sled do
  use Rustler, otp_app: :bridge_sled

  def db_open(_path), do: error()
  def db_insert(_db, _key, _value), do: error()
  def db_get(_db, _key), do: error()
  def db_apply_batch(_db, _batch), do: error()
  def db_range(_db, _min, _max, _take), do: error()

  def batch_default(), do: error()
  def batch_insert(_batch, _key, _value), do: error()
  def batch_remove(_batch, _key), do: error()

  defp error(), do: :erlang.nif_error(:nif_not_loaded)

  def test() do
    {:ok, db} = db_open("data")
    {:ok, batch} = batch_default()
    :ok = batch_insert(batch, "a", "foo")
    :ok = batch_insert(batch, "b", "foo")
    :ok = batch_insert(batch, "c", "foo")
    :ok = db_apply_batch(db, batch)
    db_range(db, "a", "z", 100)
  end
end
