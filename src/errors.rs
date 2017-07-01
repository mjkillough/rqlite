error_chain! {
    errors {
        InvalidVarint
    }


    foreign_links {
        Io(::std::io::Error);
    }
}
