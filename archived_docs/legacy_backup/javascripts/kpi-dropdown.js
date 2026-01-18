// KPI 核心指标下拉菜单功能
document.addEventListener('DOMContentLoaded', function() {
    // 查找KPI标签
    const kpiTab = document.querySelector('.md-tabs__link[href*="kpi/"]');
    
    if (!kpiTab) return;
    
    const kpiTabItem = kpiTab.parentElement;
    
    // 创建下拉菜单HTML
    const dropdownHTML = `
        <div class="kpi-dropdown">
            <a href="kpi/supply-chain.md">
                <strong>供应链 KPI</strong>
                <br><small>计划达成、库存、交付等指标</small>
            </a>
            <a href="kpi/production.md">
                <strong>生产 KPI</strong>
                <br><small>设备效率、生产周期、费用等指标</small>
            </a>
            <a href="kpi/quality.md">
                <strong>质量 KPI</strong>
                <br><small>合格率、投诉、返工等指标</small>
            </a>
            <a href="kpi/ci.md">
                <strong>持续改进 KPI</strong>
                <br><small>改善项目、成本节约、员工参与等指标</small>
            </a>
        </div>
    `;
    
    // 添加下拉菜单到KPI标签
    kpiTabItem.insertAdjacentHTML('beforeend', dropdownHTML);
    
    // 添加样式标记
    kpiTabItem.style.position = 'relative';
    
    // 鼠标悬停显示下拉菜单
    kpiTabItem.addEventListener('mouseenter', function() {
        const dropdown = this.querySelector('.kpi-dropdown');
        if (dropdown) {
            dropdown.style.opacity = '1';
            dropdown.style.visibility = 'visible';
            dropdown.style.transform = 'translateY(0)';
        }
    });
    
    kpiTabItem.addEventListener('mouseleave', function() {
        const dropdown = this.querySelector('.kpi-dropdown');
        if (dropdown) {
            dropdown.style.opacity = '0';
            dropdown.style.visibility = 'hidden';
            dropdown.style.transform = 'translateY(-10px)';
        }
    });
    
    // 点击下拉菜单项时的处理
    const dropdownLinks = kpiTabItem.querySelectorAll('.kpi-dropdown a');
    dropdownLinks.forEach(link => {
        link.addEventListener('click', function(e) {
            // 让链接正常跳转，Material会自动处理侧边栏过滤
            console.log('Navigating to:', this.href);
        });
    });
    
    // 添加键盘支持（无障碍访问）
    kpiTab.addEventListener('focus', function() {
        const dropdown = this.parentElement.querySelector('.kpi-dropdown');
        if (dropdown) {
            dropdown.style.opacity = '1';
            dropdown.style.visibility = 'visible';
            dropdown.style.transform = 'translateY(0)';
        }
    });
    
    kpiTab.addEventListener('blur', function() {
        const dropdown = this.parentElement.querySelector('.kpi-dropdown');
        if (dropdown) {
            dropdown.style.opacity = '0';
            dropdown.style.visibility = 'hidden';
            dropdown.style.transform = 'translateY(-10px)';
        }
    });
});
