package application

import (
	"time"

	"github.com/JonMunkholm/TUI/internal/admin"
	"github.com/JonMunkholm/TUI/internal/handler"
	tea "github.com/charmbracelet/bubbletea"
)

/* ----------------------------------------
	MENU TREE
---------------------------------------- */

type MenuItem struct {
	Label 		string
	Submenu		*Menu
	Action 		func() tea.Cmd
}

type Menu struct {
	Title 		string
	Items		[]MenuItem
	Parent		*Menu
}


/* ----------------------------------------
	MENU TREE DEFINITION
---------------------------------------- */

func linkParents(menu *Menu, parent *Menu) {
	menu.Parent = parent

	for i := range menu.Items {
		item := &menu.Items[i]

		if item.Label == "Back" {
			item.Submenu = parent
			continue
		}

		if item.Submenu != nil {
			linkParents(item.Submenu, menu)
		}
	}
}


func buildMenuTree(m *Model) *Menu {

	/* Submenus */
	submenuInfo := &Menu{
		Title: "Info",
		Items: []MenuItem{
			{Label: "Show DB Version", Action: func() tea.Cmd{
				return func() tea.Msg { return handler.WdMsg("Database version: placeholder")}
			}},
			{Label: "Back"},
		},
	}

	upload := loadUpload(m)
	resetDbs := loadResetDbs(m)

	/* Root Menu */
	root := &Menu{
		Title: "Main Menu",
		Items: []MenuItem{
			{Label: "Make Bubble Tea",
		Action: func() tea.Cmd {
			return func() tea.Msg {
				time.Sleep(3 * time.Second)
				return handler.DoneMsg("Bubble tea is ready!")
			}
		},
	},
	{Label: "Info ->", Submenu: submenuInfo},
	{Label: "Upload ->", Submenu: upload},
	{Label: "Reset DBs ->", Submenu: resetDbs},
		},
	}

	linkParents(root, nil)

	return root
}


/* ----------------------------------------
	LOAD MENUS
---------------------------------------- */

func loadResetDbs(m *Model) *Menu {
	resetDbsHandler := &admin.ResetDbs{DB: m.db}

	return &Menu{
		Title: "Reset DBs",
		Items: []MenuItem{
			{Label: "Reset All", Action: resetDbsHandler.ResetAll},
			{Label: "Back"},
		},
	}
}

func loadUpload(m *Model) *Menu {
	return &Menu{
		Title: "Upload",
		Items: []MenuItem{
			{Label: "NS ->", Submenu: loadNsUploadMenu(m)},
			{Label: "SFDC ->", Submenu: loadSfdcUploadMenu(m)},
			{Label: "Anrok ->", Submenu: loadAnrokUploadMenu(m)},
			{Label: "Back"},
		},
	}
}

// loadUploadMenu is a generic helper that initializes an uploader and builds a menu.
// If SetProps fails, it returns an error menu instead.
func loadUploadMenu(title string, uploader handler.Uploader, items []MenuItem) *Menu {
	if err := uploader.SetProps(); err != nil {
		return &Menu{
			Title: title,
			Items: []MenuItem{
				{Label: "Error: " + err.Error()},
				{Label: "Back"},
			},
		}
	}

	return &Menu{
		Title: title,
		Items: items,
	}
}

func loadNsUploadMenu(m *Model) *Menu {
	h := handler.NewNsUpload(m.pool)
	return loadUploadMenu("NS - Upload", h, []MenuItem{
		{Label: "Upload Customers", Action: h.InsertNsCustomers},
		{Label: "Upload SO Detail", Action: h.InsertNsSoDetail},
		{Label: "Upload Invoice Detail", Action: h.InsertNsInvoiceDetail},
		{Label: "Back"},
	})
}

func loadSfdcUploadMenu(m *Model) *Menu {
	h := handler.NewSfdcUpload(m.pool)
	return loadUploadMenu("SFDC - Upload", h, []MenuItem{
		{Label: "Upload Customers", Action: h.InsertSfdcCustomers},
		{Label: "Upload Price Book", Action: h.InsertSfdcPriceBook},
		{Label: "Upload Opps Detail", Action: h.InsertSfdcOppDetail},
		{Label: "Back"},
	})
}

func loadAnrokUploadMenu(m *Model) *Menu {
	h := handler.NewAnrokUpload(m.pool)
	return loadUploadMenu("Anrok - Upload", h, []MenuItem{
		{Label: "Upload Anrok Transactions", Action: h.InsertAnrokTransactions},
		{Label: "Back"},
	})
}
