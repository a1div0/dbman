// klenov
// 2019.12.28

package dbman

import (
    "fmt"
    "os"
    "net/http"
    "strings"
    "strconv"
    "reflect"
    "encoding/json"
    "database/sql"
    "github.com/a1div0/oauth"
)

type SqlExecutor interface {
    SqlExecute(cmd_name string, cmd_arg ...interface{}) (*sql.Rows, error)
}

type CommandParameterDescriptor struct {
    Name string `json:"name"`
    Type string `json:"type"`
    Default string `json:"default"`
}

type CommandDescriptor struct {
    CommandName string `json:"cmd_name"`
    DbProcName string `json:"db_proc_name"`
    CallMethod string `json:"call_method"`
    Parameters []CommandParameterDescriptor `json:"parameters"`
}

type DataBaseManager struct {
    executor SqlExecutor
    arg_name []string
    command_path_prefix string
    parameters_count_limit int
    cmd_descriptor_list []CommandDescriptor
}

func (db *DataBaseManager) Init(SqlExec SqlExecutor, CommandParametersFileName string, CommandPathPrefix string, ParametersCountLimit int) error {

    db.executor = SqlExec
    db.command_path_prefix = CommandPathPrefix

    file, err := os.Open(CommandParametersFileName)
    if (err != nil) {
        return err
    }

    decoder := json.NewDecoder(file)
    err = decoder.Decode(&db.cmd_descriptor_list)
    if (err != nil) {
        return err
    }

    err = db.validate_descriptor_list()
    if (err != nil) {
        return err
    }

    db.parameters_count_limit = ParametersCountLimit
    db.create_arg_names()

    return nil
}

func (db *DataBaseManager) validate_descriptor_list() error {

    support_types := make(map[string]bool)
    support_types["bool"] = true
    support_types["int"] = true
    support_types["uint"] = true
    support_types["float"] = true
    support_types["string"] = true

    command_count := len(db.cmd_descriptor_list)
    for i := 0; i < command_count; i++ {
        command_desc := &db.cmd_descriptor_list[i]
        parameters_count := len(command_desc.Parameters)
        for j := 0; j < parameters_count; j++ {
            parameter_desc := &command_desc.Parameters[j]
            _, type_support := support_types[parameter_desc.Type]
            if (!type_support) {
                return fmt.Errorf("Command '%s' parameter '%s' type '%s' not support!", command_desc.CommandName, parameter_desc.Name, parameter_desc.Type)
            }
        }
    }

    return nil
}

func (db *DataBaseManager) create_arg_names() {
    db.arg_name = make([]string, db.parameters_count_limit)
    for i := 0; i < db.parameters_count_limit-1; i++ {
        db.arg_name[i] = fmt.Sprintf("X%d", i)
    }
}

func (db *DataBaseManager) UserRegistration(u *oauth.UserData) error {

    cmd_descriptor, err := db.get_command_descriptor("#USER_REGISTER#")
    if err != nil {
        return err
    }

    cmd_arg := make([]interface{}, 4)
    cmd_arg[0] = sql.Named("user_name", u.Name)
    cmd_arg[1] = sql.Named("user_email", u.Email)
    cmd_arg[2] = sql.Named("ext_id", u.ExtId)
    cmd_arg[3] = sql.Named("oauth_service_name", u.OAuthServiceName)

    rows, err := db.executor.SqlExecute(cmd_descriptor.DbProcName, cmd_arg...)
	if err != nil {
		return err
	}
    defer rows.Close()

    if rows.Next() {
        if err := rows.Scan(&u.UserId); err != nil {
			return err
		}
    }else{
        fmt.Errorf("Command result is empty!")
    }

    return nil
}

func (db *DataBaseManager) get_command_descriptor(cmd_name string) (*CommandDescriptor, error) {

    n := len(db.cmd_descriptor_list)
    for i := 0; i < n; i++ {
        if (cmd_name == db.cmd_descriptor_list[i].CommandName) {
            return &db.cmd_descriptor_list[i], nil
        }
    }

    return nil, fmt.Errorf("Command descriptor '%s' not found!", cmd_name)
}

func (db *DataBaseManager) ExecuteCommand(w http.ResponseWriter, r *http.Request, user_id int64) (error) {

    var err error
    var cmd_arg []interface{}
    var cmd_descriptor *CommandDescriptor
    var parameters_map map[string][]string

    cmd_name := r.URL.Path[len(db.command_path_prefix):]
    cmd_descriptor, err = db.get_command_descriptor(cmd_name)
    if err != nil {
        return err
    }

    if (cmd_descriptor.CallMethod == "GET") {
        parameters_map = r.Form
    } else if (cmd_descriptor.CallMethod == "POST") {
        parameters_map = r.Form
    } else if (cmd_descriptor.CallMethod == "ORMLESS") {
        return fmt.Errorf("Unplanned Ormless-method call!")
    } else {
        return fmt.Errorf("Unknown call method!")
    }

    // fmt.Println("cmd_name=", cmd_name)
    // fmt.Println("call method:", cmd_descriptor.CallMethod)
    // fmt.Println("parameters:")
    // for k, v := range parameters_map {
    //     fmt.Println("key=", k)
    //     fmt.Println("val=", strings.Join(v, ""))
    // }

    cmd_arg, err = db.valid_and_prepare_command_arguments(cmd_descriptor, parameters_map)
    if err != nil {
        return err
    }

    arg_name0 := db.arg_name[0]
    cmd_arg[0] = sql.Named(arg_name0, user_id)

    rows, err := db.executor.SqlExecute(cmd_descriptor.DbProcName, cmd_arg...)
	if err != nil {
		return err
	}
    defer rows.Close()

    b, err := ResultToJson(rows)
    if err != nil {
		return err
	}
    fmt.Fprintf(w, string(b))

    return nil
}

func (db *DataBaseManager) valid_and_prepare_command_arguments(cmd_descriptor *CommandDescriptor, parameters_map map[string][]string) ([]interface{}, error) {

    parameters_cnt := len(parameters_map)
    if (parameters_cnt > db.parameters_count_limit) {
        return nil, fmt.Errorf("Too many parameters!")
    }

    cmd_arg_cnt := len(cmd_descriptor.Parameters) + 1 // первый аргумент всегда user_id
    cmd_arg := make([]interface{}, cmd_arg_cnt)

    for arg_id := 1; arg_id < cmd_arg_cnt; arg_id++ {

        var val_str string
        var val interface{}
        var err error

        param_descriptor := &cmd_descriptor.Parameters[arg_id - 1]
        param_val, param_exist := parameters_map[param_descriptor.Name]
        if (param_exist) {
            val_str = strings.Join(param_val, "")
        }else{
            if (param_descriptor.Default != "") {
                val_str = param_descriptor.Default
            }else{
                return cmd_arg, fmt.Errorf("Parameter '%s' not found!", param_descriptor.Name)
            }
        }

        if (param_descriptor.Type == "bool") {
            val, err = strconv.ParseBool(val_str)
        } else if (param_descriptor.Type == "int") {
            val, err = strconv.ParseInt(val_str, 10, 64)
        } else if (param_descriptor.Type == "uint") {
            val, err = strconv.ParseUint(val_str, 10, 64)
        } else if (param_descriptor.Type == "float") {
            val, err = strconv.ParseFloat(val_str, 64)
        } else if (param_descriptor.Type == "string") {
            val = val_str
            err = nil
        }

        if err != nil {
    		return cmd_arg, err
    	}

        arg_name :=  db.arg_name[arg_id]
        cmd_arg[arg_id] = sql.Named(arg_name, val)

    }

    return cmd_arg, nil
}

func ResultToJson(rows *sql.Rows) ([]byte, error) {
	// an array of JSON objects
	// the map key is the field name
	var objects []map[string]interface{}

	for rows.Next() {
		// figure out what columns were returned
		// the column names will be the JSON object field keys
		columns, err := rows.ColumnTypes()
		if err != nil {
			return nil, err
		}

		// Scan needs an array of pointers to the values it is setting
		// This creates the object and sets the values correctly
		values := make([]interface{}, len(columns))
		object := map[string]interface{}{}
		for i, column := range columns {
			object[column.Name()] = reflect.New(column.ScanType()).Interface()
			values[i] = object[column.Name()]
		}

		err = rows.Scan(values...)
		if err != nil {
			return nil, err
		}

		objects = append(objects, object)
	}

    if len(objects) == 0 {
        return []byte("{}"), nil
    }

    //data, err := json.MarshalIndent(objects, "\t", "")
    data, err := json.Marshal(&objects)
	return data, err
}
