error_chain! {
    errors {
        InvalidDbHeader(s: String) {
            description("Invalid sqlite3 database header")
            display("Invalid sqlite3 database header: {}", s)
        }
        InvalidVarint
    }


    foreign_links {
        Io(::std::io::Error);
    }
}
