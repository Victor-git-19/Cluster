( () => {
    "use strict";
    document.querySelector(".header__burger").addEventListener("click", ( () => {
        document.querySelector(".header__burger").classList.toggle("header__burger--open"),
        document.querySelector(".header").classList.toggle("header--open")
    }
    )),
    function() {
        let e = document.querySelectorAll(".select__header")
          , t = document.querySelectorAll(".select__item");
        function r() {
            this.parentElement.classList.toggle("is-active")
        }
        function c() {
            let e = this.innerText
              , t = this.closest(".select");
            t.querySelector(".select__current").innerText = e,
            t.classList.remove("is-active")
        }
        e.forEach((e => {
            e.addEventListener("click", r)
        }
        )),
        t.forEach((e => {
            e.addEventListener("click", c)
        }
        ))
    }()
}
)();
