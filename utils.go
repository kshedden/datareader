package datareader

// Statfilereader is an interface that can be used to work
// interchangeably with StataReader and SAS7BDAT objects.
type Statfilereader interface {
	ColumnNames() []string
	ColumnTypes() []int
	Read(int) ([]*Series, error)
}
