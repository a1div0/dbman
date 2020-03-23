// klenov
// 2020.01.03

package dbman

import (
    "fmt"
    "testing"
    "net/http/httptest"
    "io/ioutil"
    //"strings"
    //"fmt"
    "database/sql"
    "github.com/a1div0/fakedb"
    "github.com/a1div0/oauth"
)

// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
// sqltest



// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

type TestSqlExecutor struct {
    fc *fakedb.FakeConnector
    t *testing.T
}

func (se *TestSqlExecutor) Init(t *testing.T) {

    se.fc = &fakedb.FakeConnector{ } // name: "fakeDBName"
    se.t = t

}

func MappingArguments(cmd_arg ...interface{}) (map[string]interface{}, error) {

    arg_map := make(map[string]interface{})
    for _, arg_i := range cmd_arg {
        if named_arg, ok := arg_i.(sql.NamedArg); ok {
            arg_map[named_arg.Name] = named_arg.Value
        }else{
            return nil, fmt.Errorf("Arguments must be type is sq.NamedArg!")
        }
    }

    return arg_map, nil
}

func (se *TestSqlExecutor) SqlExecute(cmd_name string, cmd_arg ...interface{}) (*sql.Rows, error) {

    // см. тут https://golang.org/src/database/sql/sql_test.go
    //exec(t, db, "CREATE|people|name=string,age=int32,photo=blob,dead=bool,bdate=datetime")
    //exec(t, db, "INSERT|people|name=Alice,age=?,photo=APHOTO", 1)
    db := sql.OpenDB(se.fc)
    if db.Driver() != fakedb.Fdriver {
		return nil, fmt.Errorf("OpenDB should return the driver of the Connector")
	}
    if _, err := db.Exec("WIPE"); err != nil {
        return nil, fmt.Errorf("exec wipe: %v", err)
	}
    //defer sql.closeDB(se.t, db)
    defer db.Close()

    arg_map, err := MappingArguments(cmd_arg...)
    if err != nil {
        return nil, err
    }
    //fmt.Printf("arg_map = %+v\n", arg_map)

    if (cmd_name == "Entity.UserRegister") {

        user_email := arg_map["user_email"].(string)
        return se.SqlExecute__Entity_UserRegister(db, user_email)

    }else if (cmd_name == "Entity.CategoryList") {

        parent := arg_map["X1"].(int64)
        return se.SqlExecute__Entity_CategoryList(db, parent)

    }else{
        return nil, fmt.Errorf("Command '%s' not found!", cmd_name)
    }
    //fmt.Println("cmd_name=", cmd_name)
    return nil, nil
}

func (se *TestSqlExecutor) SqlExecute__Entity_CategoryList(db *sql.DB, parent int64) (*sql.Rows, error) {

    db.Exec("CREATE|categories|category_id=int64,category_parent_id=int64,category_name=string,is_folder=int32,is_minus=int32,img_url=string,sort=float64,visible=int32,is_delete=int32,last_hand_user_id=int64,project_id=int64")
    db.Exec("INSERT|categories|category_id=?,category_parent_id=?,category_name=?,is_folder=?,is_minus=?,img_url=?,sort=?,visible=?,is_delete=?,last_hand_user_id=?,project_id=?", 1, 0, "first category", 0, 0, "http://localgost/im/1.png", 1.0, 1, 0, 345, 1)

    return db.Query(
		"SELECT|categories|category_id,category_parent_id,category_name,is_folder,is_minus,img_url,sort,visible,is_delete,last_hand_user_id,project_id|category_parent_id=?parent",
		sql.Named("parent", parent),
	)

}

func (se *TestSqlExecutor) SqlExecute__Entity_UserRegister(db *sql.DB, user_email string) (*sql.Rows, error) {

    db.Exec("CREATE|users|user_email=string,user_id=int64")
    db.Exec("INSERT|users|user_email=?,user_id=?", "test@email.com", 345)

    return db.Query(
		"SELECT|users|user_id|user_email=?email",
		sql.Named("email", user_email),
	)

    // data, err := ResultToJson(rows)
    // fmt.Printf("user_email = %s\n", user_email)
    // fmt.Printf("rows = %s\n\n", string(data))
}

func dbm_init(t *testing.T, dbm *DataBaseManager, tse *TestSqlExecutor) (error) {

    tse.Init(t)

    err := dbm.Init(tse, "test.json", "/cmd/", 3)
    if (err != nil) {
        t.Error(err)
        return err
    }

    return nil
}

func TestInit(t *testing.T) {

    var (
        dbm DataBaseManager
        tse TestSqlExecutor
    )

    dbm_init(t, &dbm, &tse)
}

func TestUserRegistration(t *testing.T) {

    var (
        dbm DataBaseManager
        tse TestSqlExecutor
        u oauth.UserData
    )

    dbm_init(t, &dbm, &tse)

    u.Name = "Test User Name"
    u.Email = "test@email.com"
    u.ExtId = "T123"
    u.OAuthServiceName = "Otest"

    err := dbm.UserRegistration(&u)
    if (err != nil) {
        t.Error(err)
        return
    }

    if (u.UserId != 345) {
        t.Error("u.UserId должен быть равен 345, а он равен = ", u.UserId)
        return
    }


    // TODO
}

func TestExecuteCommand(t *testing.T) {

    var (
        dbm DataBaseManager
        tse TestSqlExecutor
    )

    dbm_init(t, &dbm, &tse)
    Request1(t, &dbm)

    body := Request2(t, &dbm, 0)
    fmt.Printf("TODO compare body=%s\n\n", string(body))

    body = Request2(t, &dbm, 1)
    if (string(body) != "{}") {
        err_msg := fmt.Sprintf("Тело ответа = '%s', а должно быть = '{}'\n", string(body))
        t.Error(err_msg)
    }
}

func Request1(t *testing.T, dbm *DataBaseManager) {

    request_url := "https://ormless.com/cmd/unknown_command?p1=1&p2=2&p3=3&p4=4&p5=5"
    r := httptest.NewRequest("GET", request_url, nil)
    r.ParseForm()

	w := httptest.NewRecorder()

    err := dbm.ExecuteCommand(w, r, 345)
    if (err.Error() != "Command descriptor 'unknown_command' not found!") {
        t.Error(err)
    }

}

func Request2(t *testing.T, dbm *DataBaseManager, parent int64) ([]byte) {

    request_url := fmt.Sprintf("https://ormless.com/cmd/entity.category_list?p=%d", parent)
    r := httptest.NewRequest("GET", request_url, nil)
    r.ParseForm()

	w := httptest.NewRecorder()

    err := dbm.ExecuteCommand(w, r, 345)
    if (err != nil) {
        t.Error(err)
        return nil
    }

    resp := w.Result()
    if (resp.StatusCode != 200) {
        t.Error("Код ответа = ", resp.StatusCode, ", а должен быть = 200\n")
    }

    body, _ := ioutil.ReadAll(resp.Body)

    return body
}
