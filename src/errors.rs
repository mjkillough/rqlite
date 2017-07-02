use types::Type;

error_chain! {
    errors {
        UnexpectedType(expected: Type, actual: Type) {
            description("Unexpected type")
            display("Unexpected type: expected {:?}, actual {:?}", expected, actual)
        }
        InvalidDbHeader(s: String) {
            description("Invalid sqlite3 database header")
            display("Invalid sqlite3 database header: {}", s)
        }
        InvalidVarint
    }


    foreign_links {
        FromUtf8(::std::string::FromUtf8Error);
        Io(::std::io::Error);
    }
}
